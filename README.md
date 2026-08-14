# tnl

Expose a local HTTP service through your own public `tnld` server. The public endpoint uses HTTPS. Fully end to end encrypted.

## Installation

Install the latest release on Linux or macOS:

Client:
```sh
curl -fsSL https://raw.githubusercontent.com/bansalarnav/tnl/main/install.sh | sh
```

Server:
```sh
curl -fsSL https://raw.githubusercontent.com/bansalarnav/tnl/main/install.sh | sh --server
```

### Build from source

Rust users can install either package directly from GitHub:

```sh
cargo install --git https://github.com/bansalarnav/tnl tnlc
cargo install --git https://github.com/bansalarnav/tnl tnld
```

## Setup

### 1. Set up the public server

The server needs Linux with systemd, a public IP address, and TCP port 443 open. Run the installer as the non-root user that should own the configuration:

```sh
curl -fsSL https://raw.githubusercontent.com/bansalarnav/tnl/main/install.sh | sh -s -- --server
```

Setup detects the server's public IP and defaults to a free `nip.io` domain. If you use your own domain, add the DNS records printed by the command. Certificate issuance requires public TCP port 443 to reach the configured listen port. Re-running the server installer upgrades the binaries and restarts the configured service.

Create credentials for a client:

```sh
tnld invite-client my-laptop
```

Copy the `tnl-login-v1...` value it prints.

### 2. Set up the local client

Install tnl on the local machine, then log in with the invitation from the server:

```sh
tnlc login 'tnl-login-v1...'
```

## Usage

If the local app is running on port 3000, expose it with:

```sh
tnlc expose 3000
```

The client prints the public HTTPS URL once it is ready. To request a memorable subdomain, add a name:

```sh
tnlc expose 3000 --name my-app
```

Keep this command running while the tunnel is in use. Manage the server with systemd:

```sh
sudo systemctl status tnld.service
sudo systemctl restart tnld.service
sudo systemctl stop tnld.service
sudo journalctl -u tnld.service -f
```

## Project structure

The workspace contains three packages:

- `tnl-core` in `core/` (imported as `tnl`): transport-neutral tunnel sessions and connection pairing
- `tnlc`: the command-line client, including HTTPS transport, ACME, and local forwarding
- `tnld`: the command-line server, including authentication, HTTP upgrades, TLS, SNI routing, and public forwarding

## Limitations

tnl currently does not work with any arbitary TCP protocol. For it to work, the protocol first must establish TLS with a `ClientHello` packet. A lot of protocols now allow you configure the connection so it works this way. For example, adding `sslnegotiation=direct` to your PostgreSQL connection string works.
