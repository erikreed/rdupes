use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use humansize::{format_size, BINARY};
use kdam::{tqdm, Bar, BarExt};
use log::{debug, info, warn};
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

    /// Processes a wave of hashing tasks.
    ///
    /// `group_id` is used to maintain grouping context across waves.
    /// Since we flatten all candidate groups into a single list to enable global
    /// disk-locality sorting, we must ensure that files are only grouped together
    /// if they matched in ALL previous hashing stages.
    ///
    /// Example:
    /// File A: StartHash "X", EndHash "Y"
    /// File B: StartHash "X", EndHash "Z"
    /// File C: StartHash "W", EndHash "Y"
    ///
    /// After Wave 1 (Start), we have Group 1: [A, B] and Group 2: [C].
    /// In Wave 2 (End), A and C both have EndHash "Y". Without `group_id`, they would
    /// be incorrectly grouped together. `group_id` ensures A is only compared with B.
    async fn process_wave(
        &self,
        tasks: Vec<(u64, usize, PathGroup)>, // (fsize, group_id, PathGroup)
        mode_fn: impl Fn(u64) -> PathCheckMode,
        desc: &str,
    ) -> HashMap<(u64, usize, Hash), Vec<PathGroup>> {
        let count = tasks.len();
        let total_size = tasks.iter().map(|(s, _, _)| *s).sum();
        let pbar = Arc::new(Mutex::new(DupeProgress::new(
            count,
            total_size,
            desc.to_string(),
        )));

        let mut results: HashMap<(u64, usize, Hash), Vec<PathGroup>> = HashMap::new();
        let mut set = JoinSet::new();

        for (fsize, group_id, group) in tasks {
            let permit = self.read_semaphore.clone().acquire_owned().await.unwrap();
            let df = self.clone();
            let mode = mode_fn(fsize);
            let path = group.paths[0].clone();
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
                (fsize, group_id, group, hash)
            });
        }

        while let Some(res) = set.join_next().await {
            let (fsize, group_id, group, hash) = res.unwrap();
            pbar.lock().await.update(fsize as usize);
            match hash {
                Ok(hash) => {
                    results.entry((fsize, group_id, hash)).or_default().push(group);
                }
                Err(e) => warn!("Failed to hash {}: {}", group.paths[0], e),
            }
        }

        drop(pbar);
        let dupes = results.values().filter(|v| v.len() > 1).count();
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
        let mut all_tasks: Vec<(u64, usize, PathGroup)> = Vec::new();
        for (fsize, groups) in size_map {
            for group in groups {
                all_tasks.push((fsize, 0, group));
            }
        }

        // Sort by inode for disk locality
        all_tasks.sort_by_key(|(_, _, group)| group.inode);

        // Wave 1: Start or Full
        let mut wave1_results = self
            .process_wave(
                all_tasks,
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

        let mut final_groups: Vec<(u64, Vec<PathGroup>)> = Vec::new();
        let mut wave2_tasks: Vec<(u64, usize, PathGroup)> = Vec::new();
        let mut next_group_id = 0;

        for ((fsize, _, _hash), groups) in wave1_results.drain() {
            if groups.len() > 1 {
                if fsize <= crate::hashing::EDGE_SIZE as u64 {
                    final_groups.push((fsize, groups));
                } else {
                    next_group_id += 1;
                    for g in groups {
                        wave2_tasks.push((fsize, next_group_id, g));
                    }
                }
            }
        }

        // Maintain disk locality order (tasks were added in order)
        wave2_tasks.sort_by_key(|(_, _, group)| group.inode);

        // Wave 2: End hashing for large files
        let mut wave2_results = self
            .process_wave(
                wave2_tasks,
                |_| PathCheckMode::End,
                "Wave 2/3 (End)",
            )
            .await;

        let mut wave3_tasks: Vec<(u64, usize, PathGroup)> = Vec::new();
        next_group_id = 0;
        for ((fsize, _, _hash), groups) in wave2_results.drain() {
            if groups.len() > 1 {
                next_group_id += 1;
                for g in groups {
                    wave3_tasks.push((fsize, next_group_id, g));
                }
            }
        }

        wave3_tasks.sort_by_key(|(_, _, group)| group.inode);

        // Wave 3: Full hashing
        let mut wave3_results = self
            .process_wave(
                wave3_tasks,
                |_| PathCheckMode::Full,
                "Wave 3/3 (Full)",
            )
            .await;

        for ((fsize, _, _hash), groups) in wave3_results.drain() {
            if groups.len() > 1 {
                final_groups.push((fsize, groups));
            }
        }

        for (fsize, groups) in final_groups {
            let num_groups = groups.len();
            let all_paths: Vec<String> = groups.into_iter().flat_map(|g| g.paths).collect();
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
