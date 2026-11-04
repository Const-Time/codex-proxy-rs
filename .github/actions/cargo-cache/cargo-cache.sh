#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 restore|snapshot BUILDER DIRECTORY amd64|arm64" >&2
  exit 2
fi
operation="$1"
builder="$2"
directory="$(realpath -m "$3")"
architecture="$4"
case "$operation" in restore|snapshot) ;; *) exit 2 ;; esac
case "$architecture" in amd64|arm64) ;; *) exit 2 ;; esac
dockerfile="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/Dockerfile"

usable() {
  # 流式检查归档中的非空依赖文件，校验时不在 runner 展开编译产物
  python3 - "$1" "$architecture" <<'PY'
import pathlib
import re
import sys
import tarfile

root = pathlib.Path(sys.argv[1])
architecture = sys.argv[2]

def contains(filename, pattern):
    with tarfile.open(root / filename, mode="r|gz") as archive:
        return any(
            member.isfile() and member.size > 0
            and re.fullmatch(pattern, member.name.removeprefix("./"))
            for member in archive
        )

try:
    valid = (
        (root / "git.tar.gz").stat().st_size > 0
        and contains("registry.tar.gz", r"cache/[^/]+/[^/]+\.crate")
        and contains("target.tar.gz", rf"{architecture}/release/deps/[^/]+\.rlib")
    )
except (OSError, tarfile.TarError):
    valid = False
sys.exit(0 if valid else 1)
PY
}

command=(docker buildx build --builder "$builder" --file "$dockerfile" --progress plain)
# mount 的内容不参与层指纹，用时间戳重跑归档操作；--no-cache 会重置 cache mount
command+=(--build-arg "CACHE_STAMP=$(date +%s%N)" --target "$operation")
valid=false
if [ "$operation" = restore ]; then
  if usable "$directory"; then
    "${command[@]}" --output type=cacheonly "$directory"
    valid=true
  else
    echo "No usable Cargo snapshot; cached image layers may still be available"
  fi
else
  # 导出只发送空上下文，新快照独立校验后替换旧目录，避免残留文件造成误判。
  mkdir -p "$(dirname "$directory")"
  temporary="$(mktemp -d "${directory}.XXXXXX")"
  trap 'rm -rf "$temporary"' EXIT
  mkdir "$temporary"/{context,snapshot}
  # tar 流避开 local exporter 的逐文件同步阻塞，只接收 /cache 下的压缩归档
  # 直接导出打包阶段，避免再创建一层归档副本
  # https://github.com/moby/buildkit/issues/2950
  "${command[@]}" --output type=tar,dest=- "$temporary/context" \
    | tar -xf - -C "$temporary/snapshot" --strip-components=1 cache/
  if usable "$temporary/snapshot"; then
    rm -rf "$directory"
    mv "$temporary/snapshot" "$directory"
    valid=true
  fi
fi

echo "Cargo snapshot for $architecture: usable=$valid"
if [ "$valid" = true ]; then du -sh "$directory"; fi
if [ -n "${GITHUB_OUTPUT:-}" ]; then printf 'usable=%s\n' "$valid" >> "$GITHUB_OUTPUT"; fi
