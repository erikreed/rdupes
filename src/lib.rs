use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::os::linux::fs::MetadataExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_fs::File;
use futures::stream::FuturesUnordered;
use futures_lite::io::BufReader;
use futures_lite::{AsyncReadExt, AsyncSeekExt};
use kdam::{tqdm, BarExt};
use log::{debug, info, warn};
use tokio::fs::symlink_metadata;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::Instant;
use walkdir::WalkDir;

const EDGE_SIZE: usize = 8096;

type Hash = [u8; 32];

pub struct DupeParams {
    pub min_file_size: u64,
    pub read_concurrency: usize,
}

pub struct DupeSet {
    pub paths: Vec<String>,
    pub fsize: u64,
}

pub struct DupeFinder {
    min_file_size: u64,
    task_semaphore: Semaphore,
    read_semaphore: Semaphore,

    dupe_files: AtomicU64,
    dupe_sizes: AtomicU64,
    dupes_tx: Sender<DupeSet>,
}

impl DupeFinder {
    pub fn new(params: DupeParams, dupes_tx: Sender<DupeSet>) -> Self {
        Self {
            min_file_size: params.min_file_size,
            task_semaphore: Semaphore::new(16),
            read_semaphore: Semaphore::new(params.read_concurrency),
            dupe_files: AtomicU64::default(),
            dupe_sizes: AtomicU64::default(),
            dupes_tx,
        }
    }

    async fn hash_file_chunk(&self, path: &str, front: bool, fsize: u64) -> std::io::Result<Hash> {
        let f = File::open(path).await?;

        let mut reader = BufReader::new(f);
        let bufsize = EDGE_SIZE.min(self.min_file_size as usize);

        let mut buf = vec![0; bufsize];

        if !front && fsize > EDGE_SIZE as u64 {
            let pos = fsize - EDGE_SIZE as u64;
            reader.seek(SeekFrom::Start(pos)).await.unwrap();
        }

        let n = reader.read(&mut buf).await?;

        let hash = blake3::hash(&buf[0..n]);
        Ok(*hash.as_bytes())
    }

    pub async fn dedupe_paths(
        &'static self,
        fsize: u64,
        paths: Vec<String>,
        mode: PathCheckMode,
    ) -> HashMap<Hash, Vec<String>> {
        let mut candidates: HashMap<Hash, Vec<String>> = HashMap::with_capacity(paths.len());

        for task in paths
            .into_iter()
            .map(|p| {
                tokio::spawn(async move {
                    let _permit = self.read_semaphore.acquire().await.unwrap();
                    let hash = match mode {
                        PathCheckMode::Start => self.hash_file_chunk(&p, true, fsize).await,
                        PathCheckMode::End => self.hash_file_chunk(&p, false, fsize).await,
                        PathCheckMode::Full => hash_file(&p),
                    };
                    (p, hash)
                })
            })
            .collect::<FuturesUnordered<_>>()
            .into_iter()
        {
            let (path, hash) = task.await.unwrap();
            match hash {
                Ok(hash) => {
                    candidates.entry(hash).or_default().push(path);
                }
                Err(e) => warn!("Failed to hash {}: {}", path, e),
            }
        }
        candidates
    }

    pub async fn traverse(
        &'static self,
        args: Vec<String>,
    ) -> Result<HashMap<u64, Vec<String>>, std::io::Error> {
        let (tx, mut rx) = mpsc::channel::<(String, std::fs::Metadata)>(1024);

        let handle = tokio::spawn(async move {
            let mut inodes = HashSet::<u64>::new();
            let mut size_map = HashMap::<u64, Vec<String>>::new();
            let mut n = 0u64;
            let mut n_skipped = 0u64;
            let mut n_hardlinks = 0u64;
            let mut size_total = 0u64;

            let mut pbar = tqdm!(desc = "Traversing files");
            while let Some((path, metadata)) = rx.recv().await {
                pbar.update(1).unwrap();

                let new_inode = inodes.insert(metadata.st_ino());

                if metadata.len() > self.min_file_size && new_inode {
                    n += 1;
                    size_total += metadata.len();
                    size_map.entry(metadata.len()).or_default().push(path);
                } else {
                    n_skipped += 1;
                }
                if !new_inode {
                    n_hardlinks += 1;
                }
            }
            eprintln!();
            info!("Files to check: {}", n);
            info!(
                "Files to check total size: {:.3} GB",
                size_total as f32 / 1024.0 / 1024.0 / 1024.0
            );
            info!("Files skipped: {}", n_skipped);
            info!("Hardlinks discovered: {}", n_hardlinks);
            info!("Unique sizes captured: {}", size_map.len());
            size_map
        });

        let num_directories = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let (spawner, waiter) = tokio_task_tracker::new();
        for entry in chain_dirs(args) {
            let tx = tx.clone();
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    info!("Failed to traverse directory. {}", e);
                    continue;
                }
            };
            let num_directories = num_directories.clone();
            let errors = errors.clone();

            spawner.spawn(|tracker| async move {
                // Move the tracker into the task.
                let _tracker = tracker;

                match symlink_metadata(entry.path()).await {
                    Ok(metadata) => {
                        if metadata.is_file() {
                            tx.send((entry.path().to_str().unwrap().to_string(), metadata))
                                .await
                                .unwrap();
                        } else {
                            num_directories.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        info!(
                            "Failed to get metadata for {}: {}",
                            entry.path().to_str().unwrap(),
                            e
                        );
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        drop(tx);

        waiter.wait().await;

        info!("Directories traversed: {:?}", num_directories);
        info!("Errors during traversal: {:?}", errors);

        let size_map = handle.await?;
        Ok(size_map)
    }

    pub async fn traverse_paths(&'static self, paths: Vec<String>) -> std::io::Result<()> {
        let now = Instant::now();
        let size_map = self.traverse(paths).await?;
        info!(
            "Traversal completed in: {:.1}s",
            now.elapsed().as_secs_f32()
        );

        let (spawner, waiter) = tokio_task_tracker::new();
        let pbar = Arc::new(Mutex::new(tqdm!(
            total = size_map.len(),
            desc = "Computing hashes"
        )));

        // TODO: remove redundant hash check for smaller file sizes: skip front/back checks
        for (fsize, paths) in size_map.into_iter() {
            let pbar = pbar.clone();

            spawner.spawn(|tracker| async move {
                let _tracker = tracker;
                let _permit = self.task_semaphore.acquire().await.unwrap();

                let candidates = self.dedupe_paths(fsize, paths, PathCheckMode::Start).await;

                for fpaths in candidates.into_values() {
                    if fpaths.len() > 1 {
                        let candidates = self.dedupe_paths(fsize, fpaths, PathCheckMode::End).await;
                        for bpaths in candidates.into_values() {
                            if bpaths.len() > 1 {
                                let candidates =
                                    self.dedupe_paths(fsize, bpaths, PathCheckMode::Full).await;
                                for dupes in candidates.into_values() {
                                    if dupes.len() > 1 {
                                        debug!("Dupes found: {:?}", dupes);
                                        self.dupe_files
                                            .fetch_add((dupes.len() - 1) as u64, Ordering::Relaxed);
                                        self.dupe_sizes.fetch_add(
                                            (dupes.len() - 1) as u64 * fsize,
                                            Ordering::Relaxed,
                                        );
                                        self.dupes_tx
                                            .send(DupeSet {
                                                fsize,
                                                paths: dupes,
                                            })
                                            .await
                                            .expect("Failed to send to dupes_tx");
                                    }
                                }
                            }
                        }
                    }
                }
                pbar.lock().await.update(1).unwrap();
            });
        }
        waiter.wait().await;
        info!("Dupes found: {:?}", self.dupe_files.load(Ordering::Relaxed));
        info!(
            "Dupes total size: {:.3} GB",
            self.dupe_sizes.load(Ordering::Relaxed) as f32 / 1024.0 / 1024.0 / 1024.0
        );
        Ok(())
    }
}

fn hash_file(path: &str) -> std::io::Result<Hash> {
    let path = path.to_owned();
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap(path)?;
    let hash = hasher.finalize();
    Ok(*hash.as_bytes())
}

fn chain_dirs(
    dirs: Vec<String>,
) -> impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>> {
    dirs.into_iter().flat_map(|s| {
        WalkDir::new(s)
            .follow_root_links(true)
            .follow_links(false)
            .max_open(1024)
    })
}

#[derive(Copy, Clone)]
pub enum PathCheckMode {
    Start,
    End,
    Full,
}