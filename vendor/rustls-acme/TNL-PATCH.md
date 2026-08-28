# Local patch

This is `rustls-acme` 0.15.4 with one behavior change in `src/state.rs`.
After notifying the ACME server that a challenge is ready, pending authorization
polls no longer notify the same challenge again. Let's Encrypt rejects that duplicate
notification with HTTP 409 while validation is already running.

Remove the workspace `[patch.crates-io]` entry and this directory once upstream
ships the same fix.
