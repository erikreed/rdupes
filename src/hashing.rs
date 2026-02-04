use std::io::Read;
use crate::types::Hash;

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
