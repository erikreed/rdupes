use std::collections::{HashMap, HashSet};
use std::env;
use std::io::SeekFrom;
use std::iter::FlatMap;
use std::os::linux::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::vec::IntoIter;

use async_fs::File;
use blake3::Hash;
use futures_lite::io::{AsyncReadExt, BufReader};
use futures_lite::{AsyncBufRead, AsyncSeekExt};
use itertools::Itertools;
use kdam::{tqdm, BarExt};
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use mimalloc::MiMalloc;
use tokio::fs::symlink_metadata;
use tokio::sync::{mpsc, Mutex, Semaphore};
use walkdir::WalkDir;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const EDGE_SIZE: usize = 8096;
const MIN_FILE_SIZE: u64 = EDGE_SIZE as u64;

lazy_static! {
    static ref TASK_SEMAPHORE: Arc<Semaphore> = Arc::new(Semaphore::new(64));
}

async fn hash_file(path: &str) -> std::io::Result<(Hash)> {
    let permit = TASK_SEMAPHORE.as_ref().acquire().await.unwrap();

    let path = path.to_owned();
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap(path)?;
    let hash = hasher.finalize();

    Ok(hash)
}

async fn hash_file_chunk(path: &str, front: bool) -> std::io::Result<(Hash)> {
    let permit = TASK_SEMAPHORE.as_ref().acquire().await.unwrap();
    let f = File::open(path).await?;

    let fsize = if front { 0 } else { f.metadata().await?.len() };
    let mut reader = BufReader::new(f);
    let mut buf = vec![0; EDGE_SIZE];

    if fsize > EDGE_SIZE as u64 {
        let pos = fsize - EDGE_SIZE as u64;
        reader.seek(SeekFrom::Start(pos)).await.unwrap();
    }

    let n = reader.read(&mut buf).await?;

    let hash = blake3::hash(&buf[0..n]);
    Ok(hash)
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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    simple_logger::SimpleLogger::new().env().init().unwrap();
    let args: Vec<String> = env::args().collect();

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, std::fs::Metadata)>();

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

            let added = inodes.insert(metadata.st_ino());

            if metadata.len() > MIN_FILE_SIZE && added {
                n += 1;
                size_total += metadata.len();
                size_map.entry(metadata.len()).or_default().push(path);
            } else {
                n_skipped += 1;
            }
            if added {
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
    let (spawner, waiter) = tokio_task_tracker::new();
    let pbar = Arc::new(Mutex::new(tqdm!(
        total = size_map.len(),
        desc = "Computing hashes"
    )));
    let dupe_files = Arc::new(AtomicU64::new(0));
    let dupe_sizes = Arc::new(AtomicU64::new(0));

    // TODO: remove redundant hash check for smaller file sizes: skip front/back checks
    for (fsize, paths) in size_map.into_iter() {
        let pbar = pbar.clone();
        let dupe_files = dupe_files.clone();
        let dupe_sizes = dupe_sizes.clone();

        spawner.spawn(|tracker| async move {
            // Move the tracker into the task.
            let _tracker = tracker;

            let mut candidates: HashMap<Hash, Vec<String>> = HashMap::with_capacity(paths.len());

            for task in paths
                .into_iter()
                .map(|p| {
                    tokio::spawn(async move {
                        let hash = hash_file_chunk(&p, true).await;
                        (p, hash)
                    })
                })
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

            for fpaths in candidates.into_values() {
                if fpaths.len() > 1 {
                    let mut candidates: HashMap<Hash, Vec<String>> =
                        HashMap::with_capacity(fpaths.len());
                    for task in fpaths
                        .into_iter()
                        .map(|p| {
                            tokio::spawn(async move {
                                let hash = hash_file_chunk(&p, false).await;
                                (p, hash)
                            })
                        })
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
                    for bpaths in candidates.into_values() {
                        if bpaths.len() > 1 {
                            let mut candidates: HashMap<Hash, Vec<String>> =
                                HashMap::with_capacity(bpaths.len());
                            for task in bpaths
                                .into_iter()
                                .map(|p| {
                                    tokio::spawn(async move {
                                        let hash = hash_file(&p).await;
                                        (p, hash)
                                    })
                                })
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
                            for dupes in candidates.into_values() {
                                if dupes.len() > 1 {
                                    debug!("Dupes found: {:?}", dupes);
                                    dupe_files
                                        .fetch_add((dupes.len() - 1) as u64, Ordering::Relaxed);
                                    dupe_sizes.fetch_add(
                                        (dupes.len() - 1) as u64 * fsize,
                                        Ordering::Relaxed,
                                    );
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
    info!("Dupes found: {:?}", dupe_files.load(Ordering::Relaxed));
    info!(
        "Dupes total size: {:.3} GB",
        dupe_sizes.load(Ordering::Relaxed) as f32 / 1024.0 / 1024.0 / 1024.0
    );

    Ok(())
}
