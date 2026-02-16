use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use humansize::{format_size, BINARY};
use kdam::{tqdm, Bar, BarExt};
use log::{debug, info, warn};
use tinyvec::TinyVec;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::hashing::{hash_file, hash_file_chunk, hash_mmap_file};
use crate::traversal::traverse_paths;
use crate::types::{DupeParams, DupeSet, Hash, PathCheckMode, PathGroup, SizeMap};

#[derive(Clone)]
pub struct DupeFinder {
    pub min_file_size: u64,
    pub read_semaphore: Arc<Semaphore>,
    pub dupe_files: Arc<AtomicU64>,
    pub dupe_sizes: Arc<AtomicU64>,
    pub read_concurrency: usize,
    pub disable_mmap: bool,
}

pub struct DupeProgress {
    pbar_count: Bar,
    pbar_size: Bar,
}

impl DupeProgress {
    pub fn new(count: usize, total_size: u64, desc: String) -> Self {
        let pbar_count = tqdm!(
            total = count,
            desc = desc.clone(),
            unit = " file",
            position = 0
        );
        let pbar_size = tqdm!(
            total = total_size as usize,
            desc = desc,
            position = 1,
            unit = "B",
            unit_scale = true
        );
        Self {
            pbar_count,
            pbar_size,
        }
    }

    pub fn update(&mut self, size: usize) {
        let _ = self.pbar_count.update(1);
        let _ = self.pbar_size.update(size);
    }
}

struct WaveResults {
    // (fsize, hash) -> list of indices into all_tasks
    groups: HashMap<(u64, Hash), TinyVec<[usize; 2]>>,
    // index into all_tasks -> hash
    hashes: HashMap<usize, Hash>,
}

impl WaveResults {
    fn new() -> Self {
        Self {
            groups: HashMap::new(),
            hashes: HashMap::new(),
        }
    }

    fn is_dupe(&self, fsize: u64, hash: &Hash) -> bool {
        self.groups.get(&(fsize, *hash)).map_or(false, |g| g.len() > 1)
    }
}

impl DupeFinder {
    pub fn new(params: DupeParams) -> Self {
        Self {
            min_file_size: params.min_file_size,
            read_semaphore: Arc::new(Semaphore::new(params.read_concurrency)),
            read_concurrency: params.read_concurrency,
            dupe_files: Arc::default(),
            dupe_sizes: Arc::default(),
            disable_mmap: params.disable_mmap,
        }
    }

    async fn process_wave(
        &self,
        all_tasks: &[(u64, PathGroup)],
        indices: Vec<usize>,
        mode_fn: impl Fn(u64) -> PathCheckMode,
        desc: &str,
    ) -> WaveResults {
        let count = indices.len();
        if count == 0 {
            return WaveResults::new();
        }

        let total_size = indices.iter().map(|&i| all_tasks[i].0).sum();
        let pbar = Arc::new(Mutex::new(DupeProgress::new(
            count,
            total_size,
            desc.to_string(),
        )));

        let mut results = WaveResults::new();
        let mut set = JoinSet::new();

        for i in indices {
            let (fsize, group) = &all_tasks[i];
            let df = self.clone();
            let mode = mode_fn(*fsize);
            let pbar = pbar.clone();
            let permit = df.read_semaphore.clone().acquire_owned().await.unwrap();
            let path = group.paths[0].clone();
            let fsize = *fsize;
            set.spawn(async move {
                let _permit = permit;
                let hash = match mode {
                    PathCheckMode::Start => hash_file_chunk(&path, true, fsize, df.min_file_size).await,
                    PathCheckMode::End => hash_file_chunk(&path, false, fsize, df.min_file_size).await,
                    PathCheckMode::Full => {
                        tokio::task::spawn_blocking(move || {
                            if df.disable_mmap {
                                hash_file(&path)
                            } else {
                                hash_mmap_file(&path)
                            }
                        })
                        .await
                        .unwrap()
                    }
                };
                pbar.lock().await.update(fsize as usize);
                (i, fsize, hash)
            });
        }

        while let Some(res) = set.join_next().await {
            let (i, fsize, hash) = res.unwrap();
            match hash {
                Ok(hash) => {
                    results.groups.entry((fsize, hash)).or_default().push(i);
                    results.hashes.insert(i, hash);
                }
                Err(e) => warn!("Failed to hash {}: {}", all_tasks[i].1.paths[0], e),
            }
        }

        let dupes = results.groups.values().filter(|v| v.len() > 1).count();
        info!(
            "{}: Processed {} files, found {} candidate groups",
            desc, count, dupes
        );

        results
    }

    pub async fn traverse_paths(&self, paths: Vec<String>) -> Result<SizeMap, std::io::Error> {
        traverse_paths(paths, self.min_file_size).await
    }

    pub fn find_duplicates(&self, size_map: SizeMap) -> tokio::sync::mpsc::Receiver<DupeSet> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let df = self.clone();
        tokio::spawn(async move {
            let _ = df.check_hashes_and_content(size_map, tx).await;
        });
        rx
    }

    pub async fn check_hashes_and_content(
        &self,
        size_map: SizeMap,
        dupes_tx: Sender<DupeSet>,
    ) -> std::io::Result<()> {
        let mut all_tasks: Vec<(u64, PathGroup)> = Vec::new();
        for (fsize, groups) in size_map {
            for group in groups {
                all_tasks.push((fsize, group));
            }
        }

        // Sort by inode for disk locality
        all_tasks.sort_by_key(|(_, group)| group.inode);

        // Wave 1: Start or Full
        let wave1_results = self
            .process_wave(
                &all_tasks,
                (0..all_tasks.len()).collect(),
                |fsize| {
                    if fsize <= crate::hashing::EDGE_SIZE as u64 {
                        PathCheckMode::Full
                    } else {
                        PathCheckMode::Start
                    }
                },
                "Wave 1/3 (Initial)",
            )
            .await;

        // Wave 2: End hashing for large files
        let wave2_indices: Vec<usize> = (0..all_tasks.len())
            .filter(|&i| {
                let (fsize, _) = &all_tasks[i];
                if let Some(hash) = wave1_results.hashes.get(&i) {
                    *fsize > crate::hashing::EDGE_SIZE as u64 && wave1_results.is_dupe(*fsize, hash)
                } else {
                    false
                }
            })
            .collect();

        let wave2_results = self
            .process_wave(
                &all_tasks,
                wave2_indices,
                |_| PathCheckMode::End,
                "Wave 2/3 (End)",
            )
            .await;

        // Wave 3: Full hashing
        let wave3_indices: Vec<usize> = (0..all_tasks.len())
            .filter(|&i| {
                let (fsize, _) = &all_tasks[i];
                if *fsize <= crate::hashing::EDGE_SIZE as u64 {
                    return false;
                }
                // Must have survived Wave 1
                if let Some(h1) = wave1_results.hashes.get(&i) {
                    if !wave1_results.is_dupe(*fsize, h1) {
                        return false;
                    }
                } else {
                    return false;
                }
                // Must have survived Wave 2
                if let Some(h2) = wave2_results.hashes.get(&i) {
                    wave2_results.is_dupe(*fsize, h2)
                } else {
                    false
                }
            })
            .collect();

        let wave3_results = self
            .process_wave(
                &all_tasks,
                wave3_indices,
                |_| PathCheckMode::Full,
                "Wave 3/3 (Full)",
            )
            .await;

        // Final results:
        // Small files from Wave 1 results that are dupes
        // Large files from Wave 3 results that are dupes
        let mut final_groups = Vec::new();
        for ((fsize, _hash), indices) in wave1_results.groups {
            if fsize <= crate::hashing::EDGE_SIZE as u64 && indices.len() > 1 {
                final_groups.push((fsize, indices));
            }
        }
        for ((fsize, _hash), indices) in wave3_results.groups {
            if indices.len() > 1 {
                final_groups.push((fsize, indices));
            }
        }

        for (fsize, indices) in final_groups {
            let num_groups = indices.len();
            let all_paths: Vec<String> = indices
                .into_iter()
                .flat_map(|i| all_tasks[i].1.paths.clone())
                .collect();
            debug!("Dupes found: {:?}", all_paths);
            self.dupe_files
                .fetch_add((all_paths.len() - 1) as u64, Ordering::Relaxed);
            self.dupe_sizes
                .fetch_add((num_groups - 1) as u64 * fsize, Ordering::Relaxed);
            let _ = dupes_tx
                .send(DupeSet {
                    fsize,
                    paths: all_paths,
                })
                .await;
        }

        info!("Dupes found: {:?}", self.dupe_files.load(Ordering::Relaxed));
        info!(
            "Dupes total size: {} (redundant data)",
            format_size(self.dupe_sizes.load(Ordering::Relaxed), BINARY)
        );
        Ok(())
    }
}
