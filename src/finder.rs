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
    groups: HashMap<(u64, Hash), TinyVec<[PathGroup; 2]>>,
    hashes: HashMap<(u64, u64), Hash>, // (fsize, inode) -> Hash
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

    async fn process_wave<I>(
        &self,
        tasks: I,
        mode_fn: impl Fn(u64) -> PathCheckMode,
        desc: &str,
    ) -> WaveResults
    where
        I: IntoIterator<Item = (u64, PathGroup)>,
    {
        let tasks: Vec<(u64, PathGroup)> = tasks.into_iter().collect();
        if tasks.is_empty() {
            return WaveResults::new();
        }

        let total_size = tasks.iter().map(|(s, _)| *s).sum();
        let pbar = Arc::new(Mutex::new(DupeProgress::new(
            tasks.len(),
            total_size,
            desc.to_string(),
        )));

        let mut results = WaveResults::new();
        let mut set = JoinSet::new();

        for (fsize, group) in tasks {
            let df = self.clone();
            let mode = mode_fn(fsize);
            let pbar = pbar.clone();
            let permit = df.read_semaphore.clone().acquire_owned().await.unwrap();
            set.spawn(async move {
                let _permit = permit;
                let p = group.paths[0].clone();
                let hash = match mode {
                    PathCheckMode::Start => hash_file_chunk(&p, true, fsize, df.min_file_size).await,
                    PathCheckMode::End => hash_file_chunk(&p, false, fsize, df.min_file_size).await,
                    PathCheckMode::Full => {
                        tokio::task::spawn_blocking(move || {
                            if df.disable_mmap {
                                hash_file(&p)
                            } else {
                                hash_mmap_file(&p)
                            }
                        })
                        .await
                        .unwrap()
                    }
                };
                pbar.lock().await.update(fsize as usize);
                (fsize, group, hash)
            });
        }

        while let Some(res) = set.join_next().await {
            let (fsize, group, hash) = res.unwrap();
            let inode = group.inode;
            match hash {
                Ok(hash) => {
                    results.groups.entry((fsize, hash)).or_default().push(group);
                    results.hashes.insert((fsize, inode), hash);
                }
                Err(e) => warn!("Failed to hash {}: {}", group.paths[0], e),
            }
        }
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
                all_tasks.iter().cloned(),
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
        let wave2_tasks: Vec<(u64, PathGroup)> = all_tasks
            .iter()
            .filter(|(fsize, group)| {
                if let Some(hash) = wave1_results.hashes.get(&(*fsize, group.inode)) {
                    *fsize > crate::hashing::EDGE_SIZE as u64 && wave1_results.is_dupe(*fsize, hash)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let wave2_results = self
            .process_wave(
                wave2_tasks,
                |_| PathCheckMode::End,
                "Wave 2/3 (End)",
            )
            .await;

        // Wave 3: Full hashing
        let wave3_tasks: Vec<(u64, PathGroup)> = all_tasks
            .iter()
            .filter(|(fsize, group)| {
                if *fsize <= crate::hashing::EDGE_SIZE as u64 {
                    return false;
                }
                // Must have survived Wave 1
                if let Some(h1) = wave1_results.hashes.get(&(*fsize, group.inode)) {
                    if !wave1_results.is_dupe(*fsize, h1) {
                        return false;
                    }
                } else {
                    return false;
                }
                // Must have survived Wave 2
                if let Some(h2) = wave2_results.hashes.get(&(*fsize, group.inode)) {
                    wave2_results.is_dupe(*fsize, h2)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let wave3_results = self
            .process_wave(
                wave3_tasks,
                |_| PathCheckMode::Full,
                "Wave 3/3 (Full)",
            )
            .await;

        // Final results:
        // Small files from Wave 1 results that are dupes
        // Large files from Wave 3 results that are dupes
        let mut final_groups = Vec::new();
        for ((fsize, _hash), groups) in wave1_results.groups {
            if fsize <= crate::hashing::EDGE_SIZE as u64 && groups.len() > 1 {
                final_groups.push((fsize, groups));
            }
        }
        for ((fsize, _hash), groups) in wave3_results.groups {
            if groups.len() > 1 {
                final_groups.push((fsize, groups));
            }
        }

        for (fsize, groups) in final_groups {
            debug!("Dupes found: {:?}", groups);
            let num_groups = groups.len();
            let all_paths: Vec<String> = groups.into_iter().flat_map(|g| g.paths).collect();
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
