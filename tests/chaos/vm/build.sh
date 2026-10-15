#!/bin/sh
set -eu
module=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
root=$(CDPATH='' cd -- "$module/../.." && pwd)
output=${1:-chaos-vm-image}
case "$output" in /*) ;; *) output="$module/$output" ;; esac
profile=${2:-standard}
architecture=${3:-$(uname -m)}
case "$architecture" in
    arm64|aarch64) architecture=arm64 ;;
    amd64|x86_64) architecture=amd64 ;;
    *) printf 'Unsupported architecture: %s (use arm64 or amd64)\n' "$architecture" >&2; exit 1 ;;
esac
case "$profile" in standard|race|asan) ;; *) printf 'Unknown check profile: %s\n' "$profile" >&2; exit 1 ;; esac
if [ -e "$output" ]; then
    printf 'Output already exists: %s\n' "$output" >&2
    exit 1
fi
mkdir -p "$output"
tag="sqlq-chaos-vm:$architecture-$profile"
docker build --platform "linux/$architecture" --build-arg "CHECK_PROFILE=$profile" -t "$tag" -f "$module/vm/Dockerfile" "$root"
container=$(docker create --platform "linux/$architecture" "$tag")
trap 'docker rm "$container" >/dev/null' EXIT HUP INT TERM
docker cp "$container:/vm/kernel" "$output/kernel"
docker cp "$container:/vm/initramfs.gz" "$output/initramfs.gz"
docker cp "$container:/vm/image.json" "$output/image.json"
printf '%s (%s) VM image written to %s\n' "$architecture" "$profile" "$output"
