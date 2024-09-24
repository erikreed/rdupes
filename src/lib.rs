#[cfg(not(unix))]
use std::collections::HashMap;
#[cfg(unix)]
use std::collections::{HashMap, HashSet};
use std::io::{Read, SeekFrom};
#[cfg(unix)]
use std::os::linux::fs::MetadataExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_fs::File;
use futures::stream::FuturesUnordered;
use futures_lite::io::BufReader;
use futures_lite::{AsyncReadExt, AsyncSeekExt};
use humansize::{format_size, BINARY};
use kdam::{tqdm, Bar, BarExt};
use log::{debug, error, info, warn};
use tinyvec::TinyVec;
use tokio::fs::symlink_metadata;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::Instant;
use walkdir::WalkDir;

const EDGE_SIZE: usize = 4096;

type Hash = [u8; 32];
type TVString = TinyVec<[String; 4]>;
type SizeMap = HashMap<u64, TVString>;

pub struct DupeParams {
    pub min_file_size: u64,
    pub read_concurrency: usize,
    pub disable_mmap: bool,
}

pub struct DupeSet {
    pub paths: Vec<String>,
    pub fsize: u64,
}

impl DupeSet {
    pub fn sort_paths(&mut self, input_paths: &[String]) {
        fn lmatch_count(left: &str, right: &str) -> usize {
            left.bytes()
                .zip(right.bytes())
                .enumerate()
                .find(|(_i, (a, b))| a != b)
                .map(|(i, _)| i)
                .unwrap_or(left.len().min(right.len()))
        }

        self.paths.sort_by_cached_key(|p| {
            input_paths
                .iter()
                .enumerate()
                .max_by_key(|(_i, s)| lmatch_count(s, p))
                .map(|(i, _s)| (i, p.len()))
                .unwrap_or((usize::MAX, usize::MAX))
        });
    }
}

#[derive(Clone)]
pub struct DupeFinder {
    min_file_size: u64,
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
        &self,
        fsize: u64,
        paths: TVString,
        mode: PathCheckMode,
    ) -> HashMap<Hash, TVString> {
        let mut candidates: HashMap<Hash, TVString> = HashMap::with_capacity(paths.len());

        for task in paths
            .into_iter()
            .map(|p| {
                let df = self.clone();
                tokio::spawn(async move {
                    let _permit = df.read_semaphore.acquire().await.unwrap();
                    let hash = match mode {
                        PathCheckMode::Start => df.hash_file_chunk(&p, true, fsize).await,
                        PathCheckMode::End => df.hash_file_chunk(&p, false, fsize).await,
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
                                .unwrap()
                        }
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

    pub async fn traverse_paths(
        &self,
        paths: Vec<String>,
    ) -> Result<SizeMap, std::io::Error> {
        let now = Instant::now();
        let (tx, mut rx) = mpsc::channel::<(String, std::fs::Metadata)>(1024);

        let df = self.clone();
        let handle = tokio::spawn(async move {
            #[cfg(unix)]
            let mut inodes = HashSet::<u64>::new();

            let mut size_map = SizeMap::new();
            let mut n = 0u64;
            let mut n_filtered = 0u64;
            let mut n_hardlinks = 0u64;
            let mut size_total = 0u64;

            let mut file_pbar = tqdm!(desc = "Traversing files", position = 0, unit = " file");
            let mut size_pbar = tqdm!(
                desc = "Traversing files",
                position = 1,
                unit = "B",
                unit_scale = true
            );
            while let Some((path, metadata)) = rx.recv().await {
                file_pbar.update(1).unwrap();
                size_pbar.update(metadata.len() as usize).unwrap();

                #[cfg(unix)]
                let new_inode = inodes.insert(metadata.st_ino());
                #[cfg(not(unix))]
                let new_inode = true;

                if new_inode {
                    if metadata.len() >= df.min_file_size {
                        n += 1;
                        size_total += metadata.len();
                        size_map.entry(metadata.len()).or_default().push(path);
                    } else {
                        n_filtered += 1;
                    }
                } else {
                    n_hardlinks += 1;
                }
            }
            drop(file_pbar);
            drop(size_pbar);
            size_map.retain(|_, v| v.len() > 1);

            info!("Files to check: {}", n);
            info!(
                "Files to check total size: {}",
                format_size(size_total, BINARY)
            );
            info!("Files size-filtered: {}", n_filtered);
            info!("Hardlinks discovered/skipped: {}", n_hardlinks);
            info!("Unique sizes captured: {}", size_map.len());
            let largest = size_map.iter().max_by_key(|(&k, v)| k * v.len() as u64);
            if let Some(largest) = largest {
                info!(
                    "Largest candidate set: {} files with total size {}",
                    largest.1.len(),
                    format_size(largest.0 * largest.1.len() as u64, BINARY)
                )
            }
            size_map
        });

        let num_directories = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let (spawner, waiter) = tokio_task_tracker::new();

        for entry in chain_dirs(paths) {
            let tx = tx.clone();
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    error!("Failed to traverse path. {}", e);
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
                            tx.send((entry.path().to_string_lossy().to_string(), metadata))
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
        info!(
            "Traversal completed in: {:.1}s",
            now.elapsed().as_secs_f32()
        );

        Ok(size_map)
    }

    pub async fn check_hashes_and_content(
        &self,
        size_map: SizeMap,
        dupes_tx: Sender<DupeSet>,
    ) -> std::io::Result<()> {
        let (spawner, waiter) = tokio_task_tracker::new();
        let pbar = Arc::new(Mutex::new(DupeProgress::new(&size_map)));
        let task_semaphore = Arc::new(Semaphore::new(self.read_concurrency.max(4)));

        // TODO: remove redundant hash check for smaller file sizes: skip front/back checks
        for (fsize, paths) in size_map.into_iter() {
            let pbar = pbar.clone();
            let dupes_tx = dupes_tx.clone();
            let permit = task_semaphore.clone().acquire_owned();
            let df = Arc::new(self.clone());
            spawner.spawn(|tracker| async move {
                let _tracker = tracker;

                let candidates = df.dedupe_paths(fsize, paths, PathCheckMode::Start).await;

                for fpaths in candidates.into_values() {
                    if fpaths.len() > 1 {
                        let candidates = df.dedupe_paths(fsize, fpaths, PathCheckMode::End).await;
                        for bpaths in candidates.into_values() {
                            if bpaths.len() > 1 {
                                let candidates =
                                    df.dedupe_paths(fsize, bpaths, PathCheckMode::Full).await;
                                for dupes in candidates.into_values() {
                                    if dupes.len() > 1 {
                                        debug!("Dupes found: {:?}", dupes);
                                        df.dupe_files
                                            .fetch_add((dupes.len() - 1) as u64, Ordering::Relaxed);
                                        df.dupe_sizes.fetch_add(
                                            (dupes.len() - 1) as u64 * fsize,
                                            Ordering::Relaxed,
                                        );
                                        dupes_tx
                                            .send(DupeSet {
                                                fsize,
                                                paths: dupes.into_iter().collect(),
                                            })
                                            .await
                                            .expect("Failed to send to dupes_tx");
                                    }
                                }
                            }
                        }
                    }
                }
                pbar.lock().await.update(fsize as usize);
                drop(permit); // move permit into the tokio thread
            });
        }
        waiter.wait().await;
        drop(pbar);
        info!("Dupes found: {:?}", self.dupe_files.load(Ordering::Relaxed));
        info!(
            "Dupes total size: {} (redundant data)",
            format_size(self.dupe_sizes.load(Ordering::Relaxed), BINARY)
        );
        Ok(())
    }
}

fn hash_mmap_file(path: &str) -> std::io::Result<Hash> {
    let path = path.to_owned();
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap(path)?;
    let hash = hasher.finalize();
    Ok(*hash.as_bytes())
}

fn hash_file(path: &str) -> std::io::Result<Hash> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0; 65536];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }

        hasher.update(&buffer[..n]);
    }

    let hash = hasher.finalize();
    Ok(*hash.as_bytes())
}

fn chain_dirs(
    dirs: Vec<String>,
) -> impl Iterator<Item=Result<walkdir::DirEntry, walkdir::Error>> {
    dirs.into_iter().flat_map(|s| {
        WalkDir::new(s)
            .follow_root_links(true)
            .follow_links(false)
            .same_file_system(true)
            .max_open(65536)
    })
}

#[derive(Copy, Clone)]
pub enum PathCheckMode {
    Start,
    End,
    Full,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorting() {
        let input_paths = vec!["/asd/".into(), "/def/".into(), "/tmp/asd/".into()];
        let mut ds = DupeSet {
            paths: vec![
                "/asd/file1000.txt".to_string(),
                "/def/z".to_string(),
                "/tmp/asd/f123.mp4".to_string(),
                "/def/yyy".to_string(),
                "/def/xx".to_string(),
                "/asd/file1.txt".to_string(),
                "/def/1234".to_string(),
                "/asd/xile100.txt".to_string(),
            ],
            fsize: 0,
        };
        ds.sort_paths(&input_paths);
        assert_eq!(
            vec![
                "/asd/file1.txt".to_string(),
                "/asd/xile100.txt".to_string(),
                "/asd/file1000.txt".to_string(),
                "/def/z".to_string(),
                "/def/xx".to_string(),
                "/def/yyy".to_string(),
                "/def/1234".to_string(),
                "/tmp/asd/f123.mp4".to_string(),
            ],
            ds.paths
        )
    }
}
