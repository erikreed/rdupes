use futures::stream::FuturesUnordered;
use futures::StreamExt;
use humansize::{format_size, BINARY};
use kdam::{tqdm, Bar, BarExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tinyvec::TinyVec;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::{Mutex, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::hashing::{hash_file, hash_mmap_file};
use crate::types::{DupeParams, DupeSet, Hash, PathCheckMode, PathGroup, SizeMap, EDGE_SIZE};

#[derive(Clone)]
pub struct DupeFinder {
    pub(crate) min_file_size: u64,
    read_semaphore: Arc<Semaphore>,
    dupe_files: Arc<AtomicU64>,
    dupe_sizes: Arc<AtomicU64>,
    read_concurrency: usize,
    disable_mmap: bool,
}

struct DupeProgress {
    pbar_count: Bar,
    pbar_size: Bar,
}

impl DupeProgress {
    fn new(size_map: &SizeMap) -> Self {
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

    fn update(&mut self, size: usize) {
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

    async fn hash_file_chunk(&self, path: &str, front: bool, fsize: u64) -> std::io::Result<Hash> {
        let f = File::open(path).await?;

        let mut reader = BufReader::new(f);
        let bufsize = EDGE_SIZE.min(self.min_file_size as usize);

        let mut buf = vec![0; bufsize];

        if !front && fsize > EDGE_SIZE as u64 {
            let pos = fsize - EDGE_SIZE as u64;
            reader.seek(SeekFrom::Start(pos)).await?;
        }

        let n = reader.read(&mut buf).await?;

        let hash = blake3::hash(&buf[0..n]);
        Ok(*hash.as_bytes())
    }

    pub async fn dedupe_paths(
        &self,
        fsize: u64,
        groups: TinyVec<[PathGroup; 2]>,
        mode: PathCheckMode,
    ) -> std::io::Result<HashMap<Hash, TinyVec<[PathGroup; 2]>>> {
        let mut candidates: HashMap<Hash, TinyVec<[PathGroup; 2]>> =
            HashMap::with_capacity(groups.len());

        let mut futures = FuturesUnordered::new();
        for g in groups {
            let df = self.clone();
            futures.push(tokio::spawn(async move {
                let _permit = df
                    .read_semaphore
                    .acquire()
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let p = &g.paths[0]; // Hash only the first path in the group
                let hash = match mode {
                    PathCheckMode::Start => df.hash_file_chunk(p, true, fsize).await,
                    PathCheckMode::End => df.hash_file_chunk(p, false, fsize).await,
                    PathCheckMode::Full => {
                        let p = p.clone();
                        tokio::task::spawn_blocking(move || {
                            if df.disable_mmap {
                                hash_file(&p)
                            } else {
                                hash_mmap_file(&p)
                            }
                        })
                        .await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                    }
                };
                Ok::<(PathGroup, std::io::Result<Hash>), std::io::Error>((g, hash))
            }));
        }

        while let Some(task) = futures.next().await {
            let (group, hash) = task.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))??;
            match hash {
                Ok(hash) => {
                    candidates.entry(hash).or_default().push(group);
                }
                Err(e) => warn!("Failed to hash {}: {}", group.paths[0], e),
            }
        }
        Ok(candidates)
    }

    pub fn find_duplicates(&self, size_map: SizeMap) -> ReceiverStream<DupeSet> {
        let (tx, rx) = mpsc::channel(32);
        let df = self.clone();
        tokio::spawn(async move {
            if let Err(e) = df.check_hashes_and_content(size_map, tx).await {
                error!("Error checking hashes and content: {}", e);
            }
        });
        ReceiverStream::new(rx)
    }

    pub async fn check_hashes_and_content(
        &self,
        size_map: SizeMap,
        dupes_tx: Sender<DupeSet>,
    ) -> std::io::Result<()> {
        let mut set = tokio::task::JoinSet::new();
        let pbar = Arc::new(Mutex::new(DupeProgress::new(&size_map)));
        let task_semaphore = Arc::new(Semaphore::new(self.read_concurrency.max(4)));

        for (fsize, groups) in size_map.into_iter() {
            let pbar = pbar.clone();
            let dupes_tx = dupes_tx.clone();
            let permit = task_semaphore.clone().acquire_owned();
            let df = Arc::new(self.clone());
            set.spawn(async move {
                if fsize <= EDGE_SIZE as u64 {
                    let candidates = df.dedupe_paths(fsize, groups, PathCheckMode::Full).await?;
                    for dgroups in candidates.into_values() {
                        let _ = report_dupes(&df, fsize, dgroups, &dupes_tx).await;
                    }
                } else {
                    let candidates = df.dedupe_paths(fsize, groups, PathCheckMode::Start).await?;

                    for fgroups in candidates.into_values() {
                        if fgroups.len() > 1 {
                            let candidates =
                                df.dedupe_paths(fsize, fgroups, PathCheckMode::End).await?;
                            for bgroups in candidates.into_values() {
                                if bgroups.len() > 1 {
                                    let candidates = df
                                        .dedupe_paths(fsize, bgroups, PathCheckMode::Full)
                                        .await?;
                                    for dgroups in candidates.into_values() {
                                        let _ = report_dupes(&df, fsize, dgroups, &dupes_tx).await;
                                    }
                                } else {
                                    let _ = report_dupes(&df, fsize, bgroups, &dupes_tx).await;
                                }
                            }
                        } else {
                            let _ = report_dupes(&df, fsize, fgroups, &dupes_tx).await;
                        }
                    }
                }
                pbar.lock().await.update(fsize as usize);
                drop(permit);
                Ok::<(), std::io::Error>(())
            });
        }
        while let Some(res) = set.join_next().await {
            res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))??;
        }
        drop(pbar);
        info!("Dupes found: {:?}", self.dupe_files.load(Ordering::Relaxed));
        info!(
            "Dupes total size: {} (redundant data)",
            format_size(self.dupe_sizes.load(Ordering::Relaxed), BINARY)
        );
        Ok(())
    }
}

async fn report_dupes(
    df: &DupeFinder,
    fsize: u64,
    groups: TinyVec<[PathGroup; 2]>,
    dupes_tx: &Sender<DupeSet>,
) -> std::io::Result<()> {
    let mut all_paths = Vec::new();
    let num_groups = groups.len();
    for g in groups {
        all_paths.extend(g.paths.into_iter());
    }

    if all_paths.len() > 1 {
        debug!("Dupes found: {:?}", all_paths);
        df.dupe_files
            .fetch_add((all_paths.len() - 1) as u64, Ordering::Relaxed);
        df.dupe_sizes
            .fetch_add((num_groups - 1) as u64 * fsize, Ordering::Relaxed);
        dupes_tx
            .send(DupeSet {
                fsize,
                paths: all_paths,
            })
            .await
            .map_err(|e| {
                error!("Failed to send to dupes_tx: {}", e);
                std::io::Error::new(std::io::ErrorKind::Other, e)
            })?;
    }
    Ok(())
}
