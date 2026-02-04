{ pkgs ? import <nixpkgs> { } }:
pkgs.rustPlatform.buildRustPackage {
  pname = "rdupes";
  version = "0.1.0";

  src = ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  meta = with pkgs.lib; {
    description = "A rust port of pydupes. Super fast.";
    homepage = "https://github.com/erikreed/rdupes";
    license = licenses.mit;
    maintainers = [ ];
  };
}
