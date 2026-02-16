use std::io::{Read, SeekFrom};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use crate::types::Hash;

pub const EDGE_SIZE: usize = 4096;

pub async fn hash_file_chunk(
    path: &str,
    front: bool,
    fsize: u64,
    min_file_size: u64,
) -> std::io::Result<Hash> {
    let f = File::open(path).await?;

    let mut reader = BufReader::new(f);
    let bufsize = EDGE_SIZE.min(min_file_size as usize);

    let mut buf = vec![0; bufsize];

    if !front && fsize > EDGE_SIZE as u64 {
        let pos = fsize - EDGE_SIZE as u64;
        reader.seek(SeekFrom::Start(pos)).await?;
    }

    let n = reader.read(&mut buf).await?;

    let hash = blake3::hash(&buf[0..n]);
    Ok(*hash.as_bytes())
}

pub fn hash_mmap_file(path: &str) -> std::io::Result<Hash> {
    let path = path.to_owned();
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap(path)?;
    let hash = hasher.finalize();
    Ok(*hash.as_bytes())
}

pub fn hash_file(path: &str) -> std::io::Result<Hash> {
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
