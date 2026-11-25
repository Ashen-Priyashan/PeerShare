# PeerShare Agent Guide

## Project context

- PeerShare is a Go 1.25+ P2P file-sharing project. The CLI supports TCP transfers, chunking, SHA-256 verification, retries, resume, LAN discovery, parallel downloads, and monitoring.
- `ui/` is a React/Vite preview. Its peer list, file list, folder selection, refresh, and download actions are simulated; do not claim they connect to the Go service.
- Keep the project educational, readable, and incremental. Reuse existing packages and the standard library before adding dependencies.
- Mobile/iPhone and cross-network rooms/signaling are future scope. Do not start them as part of unrelated work.

## Branch workflow

1. Check `git status --short --branch` and preserve unrelated edits and local data.
2. Start each task from the latest `develop`:

   ```bash
   git switch develop
   git pull --ff-only origin develop
   git switch -c feature/<short-name>
   ```

3. Never implement directly on `main` or `develop`; keep one logical feature per branch.
4. Open a PR from the feature branch into `develop`. Wait for CI to pass before merging.
5. Keep feature branches locally and remotely after merge. Merge the current branch first, then update local `develop` from `origin/develop` before creating the next branch.

## Commits and validation

- One meaningful, reviewable change per commit. Split independent work when each piece can stand on its own; avoid bulk commits and artificial micro-commits.
- Use Conventional Commit messages, for example `feat: connect UI to peer service`, `test: cover peer API`, and `docs: explain desktop setup`.
- Before **each** commit, format changed Go files with `gofmt`, then run from the repository root:

  ```bash
  go test ./...
  go vet ./...
  go build ./...
  ```

- If `ui/` changes, run from `ui/`: `npm run build` and `npm run test:sites` (run `npm ci` after checkout when dependencies are missing).
- Review `git diff --check`, `git diff`, and `git diff --cached`; confirm only intended files are staged. Stage named files, not `git add .`.
- Never commit `node_modules/`, `dist/`, `.vite/`, binaries, temporary files, received files, or local runtime data such as `shared/`, unless a task explicitly requires a tracked fixture.

## Implementation guidance

- Inspect the existing code and tests before changing behavior. Reuse `internal/protocol`, `internal/peer`, `internal/transfer`, and `internal/chunk`; do not rewrite working transfer paths without evidence.
- Use explicit, bounded framing for new protocol messages. Before changing the existing wire format, explain compatibility impact. Validate peer-supplied names, paths, metadata, and chunks before accessing or publishing files.
- Keep local UI/API services bound to loopback unless a feature explicitly needs LAN access; test and explain any wider exposure.
- Add focused tests for behavior changes and run the full relevant suites before committing.
- Update `README.md` only when user-facing behavior or project status changes. Keep its claims aligned with what runs today.
