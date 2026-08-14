#!/bin/sh

set -eu

repo="bansalarnav/tnl"
version=""
install_dir=""
modify_path=1

usage() {
  cat <<'EOF'
Usage: install.sh [options]

Options:
  --version <version>    Install a specific version, such as 0.0.1
  --install-dir <path>  Install into a custom directory
  --no-modify-path      Do not update the shell startup file
  -h, --help            Show this help
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || { echo "--version requires a value" >&2; exit 1; }
      version="${2#v}"
      shift 2
      ;;
    --install-dir)
      [ "$#" -ge 2 ] || { echo "--install-dir requires a value" >&2; exit 1; }
      install_dir="$2"
      shift 2
      ;;
    --no-modify-path)
      modify_path=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

for command in awk curl grep install tar uname; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required to install tnl." >&2
    exit 1
  fi
done

case "$(uname -s)" in
  Darwin) platform="macos" ;;
  Linux) platform="linux" ;;
  *)
    echo "tnl supports macOS and Linux." >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64) architecture="x86_64" ;;
  arm64|aarch64) architecture="aarch64" ;;
  *)
    echo "tnl supports x86_64 and ARM64 processors." >&2
    exit 1
    ;;
esac

if [ -z "$version" ]; then
  latest_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest")"
  tag="${latest_url##*/}"
  case "$tag" in
    v*) version="${tag#v}" ;;
    *)
      echo "Could not determine the latest tnl version." >&2
      exit 1
      ;;
  esac
fi

if ! printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$'; then
  echo "Invalid version: $version" >&2
  exit 1
fi

tag="v$version"
archive="tnl-$tag-$platform-$architecture.tar.gz"
download_url="https://github.com/$repo/releases/download/$tag"

if [ -z "$install_dir" ]; then
  if [ "$(id -u)" -eq 0 ]; then
    install_dir="/usr/local/bin"
  elif [ "$platform" = "macos" ] && [ -d "/usr/local/bin" ] && [ -w "/usr/local/bin" ]; then
    install_dir="/usr/local/bin"
  else
    install_dir="$HOME/.local/bin"
  fi
fi

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/tnl-install.XXXXXX")"
trap 'rm -rf "$temp_dir"' EXIT
trap 'exit 1' HUP INT TERM

echo "Downloading tnl $version for $platform $architecture..."
curl -fsSL "$download_url/$archive" -o "$temp_dir/$archive"
curl -fsSL "$download_url/SHA256SUMS" -o "$temp_dir/SHA256SUMS"

expected_checksum="$(awk -v archive="$archive" '$2 == archive { print $1 }' "$temp_dir/SHA256SUMS")"
if [ -z "$expected_checksum" ]; then
  echo "No checksum was published for $archive." >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  actual_checksum="$(sha256sum "$temp_dir/$archive" | awk '{ print $1 }')"
elif command -v shasum >/dev/null 2>&1; then
  actual_checksum="$(shasum -a 256 "$temp_dir/$archive" | awk '{ print $1 }')"
else
  echo "sha256sum or shasum is required to verify the download." >&2
  exit 1
fi

if [ "$actual_checksum" != "$expected_checksum" ]; then
  echo "Checksum verification failed for $archive." >&2
  exit 1
fi

tar -xzf "$temp_dir/$archive" -C "$temp_dir"
mkdir -p "$install_dir"
install -m 0755 "$temp_dir/tnl-$tag-$platform-$architecture/tnlc" "$install_dir/tnlc"
install -m 0755 "$temp_dir/tnl-$tag-$platform-$architecture/tnld" "$install_dir/tnld"

echo "Installed tnlc and tnld in $install_dir."

case ":${PATH:-}:" in
  *":$install_dir:"*) path_is_set=1 ;;
  *) path_is_set=0 ;;
esac

if [ "$path_is_set" -eq 0 ] && [ "$modify_path" -eq 1 ]; then
  case "$install_dir" in
    "$HOME/.local/bin") shell_path='$HOME/.local/bin' ;;
    /usr/local/bin) shell_path='/usr/local/bin' ;;
    *) shell_path='' ;;
  esac

  shell_name="$(basename "${SHELL:-sh}")"
  case "$shell_name" in
    zsh)
      profile="$HOME/.zshrc"
      path_line="export PATH=\"$shell_path:\$PATH\""
      ;;
    bash)
      if [ "$platform" = "macos" ]; then
        profile="$HOME/.bash_profile"
      else
        profile="$HOME/.bashrc"
      fi
      path_line="export PATH=\"$shell_path:\$PATH\""
      ;;
    fish)
      profile="$HOME/.config/fish/config.fish"
      path_line="fish_add_path \"$shell_path\""
      ;;
    *)
      profile="$HOME/.profile"
      path_line="export PATH=\"$shell_path:\$PATH\""
      ;;
  esac

  if [ -n "$shell_path" ]; then
    mkdir -p "$(dirname "$profile")"
    if [ ! -f "$profile" ] || ! grep -F "$path_line" "$profile" >/dev/null 2>&1; then
      printf '\n%s\n' "$path_line" >> "$profile"
      echo "Added $install_dir to PATH in $profile."
    fi
    echo "Restart your shell, then run: tnlc --version"
  else
    echo "$install_dir is not in PATH. Add it before using tnlc or tnld."
  fi
elif [ "$path_is_set" -eq 0 ]; then
  echo "$install_dir is not in PATH. Add it before using tnlc or tnld."
else
  echo "Run: tnlc --version"
fi
