## rdupes
rdupes is a fast duplicate file finder. 

A port of [pydupes](https://github.com/erikreed/pydupes)

### Usage/install
```sh
# Collect counts and stage the duplicate files, null-delimited source-target pairs:
cargo run -- /path1 /path2 > dupes.txt

# Sanity check a hardlinking of all matches:
xargs -0 -n2 echo ln --force --verbose < dupes.txt
```
