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
    pub fn new(size_map: &SizeMap) -> Self {
        let pbar_count = tqdm!(
            total = size_map.len(),
            desc = "Computing hashes",
            unit = " file",
            position = 0
        );
        let pbar_size = tqdm!(
            total = size_map.keys().sum::<u64>() as usize,
            desc = "Computing hashes",
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
        self.pbar_count.update(1).unwrap();
        self.pbar_size.update(size).unwrap();
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

    pub async fn dedupe_paths<I>(
        &self,
        fsize: u64,
        groups: I,
        mode: PathCheckMode,
    ) -> HashMap<Hash, TinyVec<[PathGroup; 2]>>
    where
        I: IntoIterator<Item = PathGroup>,
    {
        let mut candidates: HashMap<Hash, TinyVec<[PathGroup; 2]>> = HashMap::new();
        let mut set = JoinSet::new();

        for group in groups {
            let df = self.clone();
            set.spawn(async move {
                let _permit = df.read_semaphore.acquire().await.unwrap();
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
                (group, hash)
            });
        }

        while let Some(res) = set.join_next().await {
            let (group, hash) = res.unwrap();
            match hash {
                Ok(hash) => {
                    candidates.entry(hash).or_default().push(group);
                }
                Err(e) => warn!("Failed to hash {}: {}", group.paths[0], e),
            }
        }
        candidates
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
        let mut set = JoinSet::new();
        let pbar = Arc::new(Mutex::new(DupeProgress::new(&size_map)));
        let task_semaphore = Arc::new(Semaphore::new(self.read_concurrency.max(4)));

        for (fsize, groups) in size_map.into_iter() {
            let pbar = pbar.clone();
            let dupes_tx = dupes_tx.clone();
            let permit = task_semaphore.clone().acquire_owned();
            let df = Arc::new(self.clone());
            set.spawn(async move {
                let _permit_move = permit;

                let candidates = if fsize <= crate::hashing::EDGE_SIZE as u64 {
                    df.dedupe_paths(fsize, groups, PathCheckMode::Full).await
                } else {
                    let mut final_candidates = HashMap::new();
                    let start_candidates = df.dedupe_paths(fsize, groups, PathCheckMode::Start).await;
                    for fgroups in start_candidates.into_values() {
                        if fgroups.len() > 1 {
                            let end_candidates = df.dedupe_paths(fsize, fgroups, PathCheckMode::End).await;
                            for bgroups in end_candidates.into_values() {
                                if bgroups.len() > 1 {
                                    let full_candidates = df.dedupe_paths(fsize, bgroups, PathCheckMode::Full).await;
                                    for (hash, dupes) in full_candidates {
                                        if dupes.len() > 1 {
                                            final_candidates.insert(hash, dupes);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    final_candidates
                };

                for dupes in candidates.into_values() {
                    if dupes.len() > 1 {
                        debug!("Dupes found: {:?}", dupes);
                        let num_groups = dupes.len();
                        let all_paths: Vec<String> =
                            dupes.into_iter().flat_map(|g| g.paths).collect();
                        df.dupe_files.fetch_add(
                            (all_paths.len() - 1) as u64,
                            Ordering::Relaxed,
                        );
                        df.dupe_sizes.fetch_add(
                            (num_groups - 1) as u64 * fsize,
                            Ordering::Relaxed,
                        );
                        let _ = dupes_tx
                            .send(DupeSet {
                                fsize,
                                paths: all_paths,
                            })
                            .await;
                    }
                }
                pbar.lock().await.update(fsize as usize);
            });
        }
        while let Some(_) = set.join_next().await {}
        drop(pbar);
        info!("Dupes found: {:?}", self.dupe_files.load(Ordering::Relaxed));
        info!(
            "Dupes total size: {} (redundant data)",
            format_size(self.dupe_sizes.load(Ordering::Relaxed), BINARY)
        );
        Ok(())
    }
}
