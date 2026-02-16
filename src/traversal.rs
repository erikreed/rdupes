use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use humansize::{format_size, BINARY};
use kdam::{tqdm, BarExt};
use log::{error, info};
use tokio::fs::symlink_metadata;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use walkdir::WalkDir;

use crate::types::{FileKey, PathGroup, SizeMap, TVString};

pub async fn traverse_paths(
    paths: Vec<String>,
    min_file_size: u64,
) -> Result<SizeMap, std::io::Error> {
    let now = Instant::now();
    let (tx, mut rx) = mpsc::channel::<(String, std::fs::Metadata)>(1024);

    let handle = tokio::spawn(async move {
        #[cfg(unix)]
        let mut size_inode_to_idx = HashMap::<FileKey, usize>::new();

        let mut size_map = SizeMap::new();
        let mut n = 0u64;
        let mut n_filtered = 0u64;
        #[cfg(unix)]
        let mut n_hardlinks = 0u64;
        #[cfg(not(unix))]
        let n_hardlinks = 0u64;
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

            let fsize = metadata.len();
            #[cfg(unix)]
            let inode = metadata.ino();

            if fsize >= min_file_size {
                #[cfg(unix)]
                {
                    if let Some(&idx) = size_inode_to_idx.get(&(fsize, inode)) {
                        if let Some(groups) = size_map.get_mut(&fsize) {
                            groups[idx].paths.push(path);
                            n_hardlinks += 1;
                        }
                    } else {
                        let groups = size_map.entry(fsize).or_default();
                        size_inode_to_idx.insert((fsize, inode), groups.len());
                        let mut paths = TVString::default();
                        paths.push(path);
                        groups.push(PathGroup { paths, inode });
                        n += 1;
                        size_total += fsize;
                    }
                }
                #[cfg(not(unix))]
                {
                    let groups = size_map.entry(fsize).or_default();
                    let mut paths = TVString::default();
                    paths.push(path);
                    groups.push(PathGroup {
                        paths,
                        inode: 0,
                    });
                    n += 1;
                    size_total += fsize;
                }
            } else {
                n_filtered += 1;
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
        let largest = size_map.iter().max_by_key(|&(&k, ref v)| k * v.len() as u64);
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

    let mut set = JoinSet::new();

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

        set.spawn(async move {
            match symlink_metadata(entry.path()).await {
                Ok(metadata) => {
                    if metadata.is_file() {
                        let _ = tx
                            .send((entry.path().to_string_lossy().to_string(), metadata))
                            .await;
                    } else {
                        num_directories.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    info!(
                        "Failed to get metadata for {}: {}",
                        entry.path().to_string_lossy(),
                        e
                    );
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
    drop(tx);

    while let Some(_) = set.join_next().await {}

    eprintln!();
    eprintln!();
    info!("Directories traversed: {:?}", num_directories);
    info!("Errors during traversal: {:?}", errors);

    let size_map = handle.await?;
    info!(
        "Traversal completed in: {:.1}s",
        now.elapsed().as_secs_f32()
    );

    Ok(size_map)
}

pub fn chain_dirs(
    dirs: Vec<String>,
) -> impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>> {
    dirs.into_iter().flat_map(|s| {
        WalkDir::new(s)
            .follow_root_links(true)
            .follow_links(false)
            .same_file_system(true)
            .max_open(65536)
    })
}
