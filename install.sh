#!/bin/sh

set -eu

repo="bansalarnav/tnl"
version=""
install_dir=""
modify_path=1
server_mode=0
service_user=""

usage() {
  cat <<'EOF'
Usage: install.sh [options]

Options:
  --version <version>    Install a specific version, such as 0.0.1
  --install-dir <path>  Install into a custom directory
  --no-modify-path      Do not update the shell startup file
  --server              Install tnld as a systemd service on Linux
  --service-user <user> Run the systemd service as this existing user
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
    --server)
      server_mode=1
      shift
      ;;
    --service-user)
      [ "$#" -ge 2 ] || { echo "--service-user requires a value" >&2; exit 1; }
      service_user="$2"
      shift 2
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

if [ "$server_mode" -eq 1 ]; then
  if [ "$platform" != "linux" ]; then
    echo "The tnld system service is supported only on Linux." >&2
    exit 1
  fi
  if ! command -v systemctl >/dev/null 2>&1 || [ ! -d /run/systemd/system ]; then
    echo "--server requires a Linux system running systemd." >&2
    exit 1
  fi
  if [ -n "$install_dir" ] && [ "$install_dir" != "/usr/local/bin" ]; then
    echo "--server installs binaries in /usr/local/bin; remove --install-dir." >&2
    exit 1
  fi

  if [ -z "$service_user" ]; then
    if [ "$(id -u)" -eq 0 ]; then
      if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
        service_user="$SUDO_USER"
      else
        echo "Run the installer as the non-root user that should own tnld, or pass --service-user." >&2
        exit 1
      fi
    else
      service_user="$(id -un)"
    fi
  fi

  if ! id "$service_user" >/dev/null 2>&1; then
    echo "Service user $service_user does not exist." >&2
    exit 1
  fi
  if ! printf '%s\n' "$service_user" | grep -Eq '^[A-Za-z_][A-Za-z0-9_.-]*$'; then
    echo "Service user contains unsupported characters: $service_user" >&2
    exit 1
  fi
  if [ "$(id -u "$service_user")" -eq 0 ]; then
    echo "tnld must run as a non-root service user." >&2
    exit 1
  fi

  service_group="$(id -gn "$service_user")"
  if ! printf '%s\n' "$service_group" | grep -Eq '^[A-Za-z_][A-Za-z0-9_.-]*$'; then
    echo "Service group contains unsupported characters: $service_group" >&2
    exit 1
  fi
  service_home="$(awk -F: -v user="$service_user" '$1 == user { print $6 }' /etc/passwd)"
  if [ -z "$service_home" ] || [ ! -d "$service_home" ] || \
    ! printf '%s\n' "$service_home" | grep -Eq '^/[A-Za-z0-9_./-]+$'; then
    echo "Could not find a home directory for $service_user." >&2
    exit 1
  fi

  if [ "$(id -u)" -ne 0 ] && ! command -v sudo >/dev/null 2>&1; then
    echo "sudo is required to install the systemd service." >&2
    exit 1
  fi
  install_dir="/usr/local/bin"
  modify_path=0
fi

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

run_privileged() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo "$@"
  fi
}

run_as_service_user() {
  case "${1:-}" in
    setup|stop) ;;
    *) echo "Unsupported tnld service action: ${1:-}" >&2; return 1 ;;
  esac

  if [ "$(id -un)" = "$service_user" ]; then
    HOME="$service_home" /usr/local/bin/tnld "$@"
  elif [ "$(id -u)" -eq 0 ]; then
    su -s /bin/sh -c "HOME='$service_home' /usr/local/bin/tnld $*" "$service_user"
  else
    sudo -u "$service_user" -H /usr/local/bin/tnld "$@"
  fi
}

if [ "$server_mode" -eq 1 ]; then
  run_privileged mkdir -p "$install_dir"
  run_privileged install -m 0755 "$temp_dir/tnl-$tag-$platform-$architecture/tnlc" "$install_dir/tnlc"
  run_privileged install -m 0755 "$temp_dir/tnl-$tag-$platform-$architecture/tnld" "$install_dir/tnld"
else
  mkdir -p "$install_dir"
  install -m 0755 "$temp_dir/tnl-$tag-$platform-$architecture/tnlc" "$install_dir/tnlc"
  install -m 0755 "$temp_dir/tnl-$tag-$platform-$architecture/tnld" "$install_dir/tnld"
fi

echo "Installed tnlc and tnld in $install_dir."

if [ "$server_mode" -eq 1 ]; then
  cat > "$temp_dir/tnld.service" <<EOF
[Unit]
Description=tnl tunnel server
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$service_user
Group=$service_group
Environment=HOME=$service_home
ExecStart=/usr/local/bin/tnld start
Restart=on-failure
RestartSec=5s
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$service_home/.tnld
UMask=0077

[Install]
WantedBy=multi-user.target
EOF

  run_privileged install -m 0644 "$temp_dir/tnld.service" /etc/systemd/system/tnld.service
  run_privileged systemctl daemon-reload

  if [ ! -f "$service_home/.tnld/config.json" ]; then
    echo
    echo "Configuring tnld as $service_user..."
    if [ ! -r /dev/tty ]; then
      echo "Server setup requires an interactive terminal." >&2
      echo "Run /usr/local/bin/tnld setup as $service_user, then rerun this installer." >&2
      exit 1
    fi
    run_as_service_user setup </dev/tty
  fi

  if [ -f "$service_home/.tnld/server.pid" ]; then
    echo "Stopping the previous background server..."
    if ! run_as_service_user stop; then
      echo "The previous PID was stale; continuing with systemd."
    fi
  fi

  run_privileged systemctl enable tnld.service
  run_privileged systemctl restart tnld.service
  echo "Started the tnld system service."
  echo "View logs with: sudo journalctl -u tnld.service -f"
  exit 0
fi

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
