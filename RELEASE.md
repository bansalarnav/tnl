# Releasing tnl

Run the release script from a clean, up-to-date `main` branch:

```sh
./release 0.2.0
```

Prerelease versions such as `./release 0.2.0-rc.1` are also supported.

The script updates the shared Cargo workspace version and lockfile, commits and pushes any version change, creates a draft GitHub release with a linked list of every commit since the previous tag, starts the GitHub Actions workflow, and watches it to completion.

The workflow verifies that the tag matches all three Cargo package versions, pins the draft to the selected `main` commit, and builds both `tnlc` and `tnld` for:

- Linux x86_64
- Linux ARM64
- macOS x86_64
- macOS ARM64

After every build succeeds, it attaches the archives and `SHA256SUMS`, creates the Git tag, and publishes the draft. If a build fails, the draft remains unpublished. Rerun `./release` with the same version after fixing and merging the problem.
