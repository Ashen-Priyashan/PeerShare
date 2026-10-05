# PeerShare

A peer-to-peer (P2P) file sharing system built with **Go**.

PeerShare transfers files over TCP in 1 MiB chunks. It can discover peers on a local network, download chunks concurrently from matching peers, and verify chunk and file contents with SHA-256.

The project focuses on learning and implementing concepts from **computer networking, distributed systems, and concurrent programming**.

---

## How It Works

Instead of downloading an entire file from a single server:

```text
              Traditional
                  Server
                    |
                    v
                  File
                    |
                    v
                  You
```

PeerShare distributes the file across multiple peers:

```text
              +-------- Peer A
              |
You ----------+-------- Peer B
              |
              +-------- Peer C
```

A large file is split into chunks:

```text
File
 |
 +-- Chunk 0
 +-- Chunk 1
 +-- Chunk 2
 +-- Chunk 3
 +-- Chunk 4
```

The requester compares file metadata and whole-file digests from discovered peers, then distributes chunk requests across up to eight matching peers. The receiver verifies each chunk and the completed file before publishing the result.

---

## Current Features

- **Peer-to-peer communication** using TCP sockets
- **Length-prefixed JSON protocol messages** with bounded message sizes
- **Chunked file transfer** over TCP in 1 MiB pieces
- **Chunk reconstruction** with validation of indices, sizes, and ordering
- **Per-chunk SHA-256 checks** before a received file is published
- **Concurrent inbound peer transfers** handled independently by the server
- **LAN peer discovery** over UDP broadcast, with direct-address connection as a fallback
- **Parallel chunk downloads** from up to eight peers advertising the same file digest
- **Failed-chunk retries** across compatible peers, with chunk verification before writing
- **Resumable downloads** that retain verified chunks across interrupted runs
- **Transfer summary** with file size, chunk count, candidate peers, elapsed time, and average throughput
- **Latency-ranked peer selection** across up to 32 probed candidates, limited to matching file digests
- **Peer monitoring** with repeated LAN discovery and protocol-level health checks
- **CLI validation** with actionable usage errors and configurable shared directories

---

## Technology Stack

| Technology | Purpose |
|------------|---------|
| Go | Main programming language |
| TCP | Peer communication and file transfer |
| UDP | LAN peer discovery |
| JSON | Framed protocol control messages |
| SHA-256 | Verify chunks and completed files |
| Go concurrency | Serve peers and download chunks concurrently |
| Git & GitHub | Version control |

---

## Project Structure

```text
PeerShare/
│
├── cmd/
│   └── peer/
│       └── main.go
│
├── internal/
│   ├── chunk/
│   ├── peer/
│   ├── protocol/
│   └── transfer/
│
├── README.md
└── go.mod
```

`peer` handles TCP serving, UDP discovery, and peer health checks. `transfer` plans, verifies, retries, resumes, and assembles file chunks. `protocol` defines the framed JSON messages shared by peers.

---

## Roadmap

### Phase 1 — Basic Networking

- [x] Initial Go project setup
- [x] TCP server and client
- [x] Peer-to-peer connection
- [x] Basic message exchange

### Phase 2 — File Sharing

- [x] File transfer
- [x] File chunking
- [x] Chunk reconstruction
- [x] Length-prefixed protocol messages

### Phase 3 — Distributed Sharing

- [x] Multiple concurrent peer connections to one server
- [x] LAN peer discovery
- [x] Parallel chunk downloads from matching peers

### Phase 4 — Reliability

- [x] SHA-256 verification for chunks and completed files
- [x] Retry failed chunks from compatible peers
- [x] Resume interrupted downloads using verified partial chunks

### Phase 5 — Improvements

- [x] Transfer statistics
- [x] Concurrent peer probing and latency-based selection
- [x] Network monitoring for discovered peer reachability
- [x] Improved CLI commands and error reporting

---

## Current Status

**Desktop prototype — Phases 1–5 complete**

The server accepts multiple clients and serves files over TCP on port 5000 by default. UDP discovery listens on port 5001. Clients compare file size and SHA-256 identity, select responsive matching peers, then request indexed chunks over up to eight connections. Control messages use a 4-byte big-endian length prefix; raw chunk payload sizes come from their metadata.

The desktop prototype supports:

- Single- and multi-peer downloads with chunk and whole-file SHA-256 checks
- Failed-chunk retries and resume after interruption
- Throughput summaries and a monitor command for peer health
- Direct peer addresses when UDP broadcast is unavailable

Interrupted downloads keep a `.peershare.partial` file and a `.peershare.state` manifest beside the requested output. On a later run, PeerShare checks the file identity and rehashes saved chunks before reusing them. Both files are removed when the verified download is published.

Use `peer monitor` to periodically discover peers and check protocol-level health over TCP. Pass `-once` for a single scan, or configure `-discover`, `-interval`, `-discover-timeout`, and `-probe-timeout` for a monitoring session.

All planned desktop roadmap items are complete. Mobile/iPhone support remains future work.

---

## Getting Started

### Requirements

- Go 1.25 or later
- Git

### Clone the Repository

```bash
git clone https://github.com/Ashen-Priyashan/PeerShare.git
cd PeerShare
```

### Run the local transfer demo

Create a file in the server's shared directory:

```bash
mkdir -p shared
printf 'Hello from PeerShare\n' > shared/test.txt
```

Start the server in one terminal:

```bash
go run ./cmd/peer server -shared shared
```

Start the client in another terminal from the repository directory:

```bash
go run ./cmd/peer client
```

The client discovers a peer over the local network and saves the received file as `received_test.txt`. To connect directly instead, pass the server address:

```bash
go run ./cmd/peer client -peer 192.168.1.42:5000 -file test.txt -output received_test.txt
```

Check LAN peer availability with a single scan:

```bash
go run ./cmd/peer monitor -once
```

---

## Learning Goals

This project demonstrates practical experience with:

- Computer networking
- TCP/UDP sockets
- Distributed systems
- Concurrent programming
- File handling
- Cryptographic hashing
- Network protocol design

---

## Inspiration

PeerShare is inspired by the concepts behind **P2P file-sharing systems such as BitTorrent**, but is being implemented as a smaller learning project from the ground up.
