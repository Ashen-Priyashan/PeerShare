# PeerShare

A peer-to-peer (P2P) file sharing system built with **Go**.

PeerShare currently transfers files between two peers over TCP in ordered 1 MiB chunks. The longer-term goal is to add multi-peer downloads, peer discovery, and cryptographic integrity verification.

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

Currently, one peer requests chunks in order from another peer. Each chunk has an index and byte size, and the receiver writes the chunks to a temporary file before publishing the completed file. Downloading chunks from different peers and verifying their hashes are planned features.

---

## Current Features

- **Peer-to-peer communication** using TCP sockets
- **Length-prefixed JSON protocol messages** with bounded message sizes
- **Single-peer file transfer** in ordered 1 MiB chunks
- **Chunk reconstruction** with validation of indices, sizes, and ordering

Planned features include:

- **SHA-256 verification** for chunk integrity
- **Parallel downloads** from multiple peers
- **Peer discovery**
- **Concurrent file transfers**
- **Retry handling** for failed chunks
- **Resume interrupted downloads**

---

## Technology Stack

| Technology | Purpose |
|------------|---------|
| Go | Main programming language |
| TCP | Peer communication and file transfer |
| SHA-256 | Planned file and chunk integrity verification |
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
│   ├── protocol/
│   └── transfer/
│
├── README.md
└── go.mod
```

The structure will evolve as new features are implemented.

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

- [ ] Multiple peer connections
- [ ] Peer discovery
- [ ] Parallel chunk downloads

### Phase 4 — Reliability

- [ ] SHA-256 verification
- [ ] Failed chunk retry
- [ ] Resume interrupted downloads

### Phase 5 — Improvements

- [ ] Transfer statistics
- [ ] Better peer selection
- [ ] Network monitoring
- [ ] Improved CLI

---

## Current Status

🚧 **Early Development**

The current demo supports one client and one server peer at a time. The client requests a file, receives its metadata, then requests each chunk in order. The server responds with chunk metadata followed by exactly that chunk's bytes. JSON control messages use a 4-byte big-endian length prefix; the raw payload length is carried in the metadata.

Currently, PeerShare can:

- Start a peer as a TCP server
- Connect to another peer as a TCP client
- Establish TCP connections between peers
- Exchange length-prefixed JSON protocol messages
- Request files from another peer
- Send and receive ordered chunks over TCP
- Reconstruct the received file and publish it after the full transfer completes

SHA-256 verification, multiple peer connections, peer discovery, and parallel downloads are not implemented yet.

---

## Getting Started

### Requirements

- Go 1.26 or later
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
go run ./cmd/peer server
```

Start the client in another terminal from the repository directory:

```bash
go run ./cmd/peer client
```

The client connects to `localhost:5000` and saves the received file as `received_test.txt`.

---

## Learning Goals

This project is being built to gain practical experience with:

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

---

## Status

🚧 Work in Progress
