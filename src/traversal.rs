use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use humansize::{format_size, BINARY};
use kdam::{tqdm, BarExt};
use log::{error, info};
use tokio::fs::symlink_metadata;
use tokio::sync::mpsc;
use tokio::time::Instant;
use walkdir::WalkDir;

use crate::types::SizeMap;
use crate::finder::DupeFinder;

pub fn chain_dirs(
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

impl DupeFinder {
    pub async fn traverse_paths(
        &self,
        paths: Vec<String>,
    ) -> Result<SizeMap, std::io::Error> {
        let now = Instant::now();
        let (tx, mut rx) = mpsc::channel::<(String, std::fs::Metadata)>(1024);

        let df = self.clone();
        let handle = tokio::spawn(async move {
            let mut file_map: HashMap<crate::types::FileKey, crate::types::PathGroup> = HashMap::new();
            let mut n_filtered = 0u64;

            let mut file_pbar = tqdm!(desc = "Traversing files", position = 0, unit = " file");
            let mut size_pbar = tqdm!(
                desc = "Traversing files",
                position = 1,
                unit = "B",
                unit_scale = true
            );
            while let Some((path, metadata)) = rx.recv().await {
                let fsize = metadata.len();
                let _ = file_pbar.update(1);
                let _ = size_pbar.update(fsize as usize);

                if fsize >= df.min_file_size {
                    #[cfg(unix)]
                    let inode = metadata.ino();
                    #[cfg(not(unix))]
                    let inode = 0; // Simplified for non-unix, but could be improved

                    file_map
                        .entry((fsize, inode))
                        .or_default()
                        .paths
                        .push(path);
                } else {
                    n_filtered += 1;
                }
            }
            drop(file_pbar);
            drop(size_pbar);

            let mut size_map = SizeMap::new();
            let mut n = 0u64;
            let mut n_hardlinks = 0u64;
            let mut size_total = 0u64;

            for ((fsize, _inode), group) in file_map {
                n += 1;
                size_total += fsize;
                n_hardlinks += (group.paths.len() - 1) as u64;
                size_map.entry(fsize).or_default().push(group);
            }

            size_map.retain(|_, v| {
                v.len() > 1 || (v.len() == 1 && v[0].paths.len() > 1)
            });

            info!("Unique files to check: {}", n);
            info!(
                "Unique files to check total size: {}",
                format_size(size_total, BINARY)
            );
            info!("Files size-filtered: {}", n_filtered);
            info!("Hardlinks discovered: {}", n_hardlinks);
            info!("Unique sizes captured: {}", size_map.len());

            size_map
        });

        let num_directories = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let mut set = tokio::task::JoinSet::new();

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
                            let _ = tx.send((entry.path().to_string_lossy().to_string(), metadata))
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

        info!("Directories traversed: {:?}", num_directories);
        info!("Errors during traversal: {:?}", errors);

        let size_map = handle.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        info!(
            "Traversal completed in: {:.1}s",
            now.elapsed().as_secs_f32()
        );

        Ok(size_map)
    }
}
