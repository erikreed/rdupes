## rdupes
rdupes is a fast duplicate file finder. 

A port of [pydupes](https://github.com/erikreed/pydupes).

### Usage/install
```sh
# Collect counts and stage the duplicate files, null-delimited source-target pairs:
cargo run -- /path1 /path2 --output-path dupes.txt

# Sanity check a hardlinking of all matches:
xargs -0 -n2 echo ln --force --verbose < dupes.txt
```

### Help
```
 cargo run -- --help
A rust port of pydupes. Super fast.

Usage: ./rdupes [OPTIONS] [PATHS]...

Arguments:
  [PATHS]...  Paths to traverse

Options:
  -o, --output-path <OUTPUT_PATH>
          Save null delimited source-dupe pairs to the output path. Use '-' for stdout
  -s, --checkpoint-save-path <CHECKPOINT_SAVE_PATH>
          Save a traversal checkpoint at the path if provided. Use '-' for stdout
  -l, --checkpoint-load-path <CHECKPOINT_LOAD_PATH>
          Load a traversal checkpoint at the path if provided
  -m, --min-file-size <MIN_FILE_SIZE>
          Filter for files of at least the minimum size [default: 4096]
  -r, --read-concurrency <READ_CONCURRENCY>
          I/O concurrency to use for reads. For SSDs, a higher value like 128 is reasonable, while HDDs should be very low, probably 1 if files are very large on average (multi-GB) [default: 4]
  -d, --disable-mmap
          Disable memory-mapped I/O for full file hashing. This helps memory benchmarking but may reduce hash speed for files already in the OS cache
  -v, --verbose
          Enable verbose/debug logging
  -h, --help
          Print help
  -V, --version
          Print version
```
