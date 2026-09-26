# PeerShare

A peer-to-peer (P2P) file sharing system built with **Go**.

PeerShare is designed to allow computers to share files directly with each other. Large files are divided into smaller chunks, which can be downloaded from multiple peers and verified using cryptographic hashes.

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

Different peers can provide different chunks. Once the required chunks are downloaded, they are verified and combined to reconstruct the original file.

---

## Key Features

Planned core features include:

- **Peer-to-peer communication** using TCP/UDP sockets
- **File chunking** for large files
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
| TCP / UDP | Network communication |
| SHA-256 | File and chunk integrity verification |
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
│   ├── network/
│   ├── protocol/
│   ├── peer/
│   ├── chunk/
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

- [ ] File transfer
- [ ] File chunking
- [ ] Chunk reconstruction

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

The initial P2P communication layer has been implemented.

Currently, PeerShare can:

- Start a peer as a TCP server
- Connect to another peer as a TCP client
- Establish a TCP connection between peers
- Exchange messages in both directions
- Use structured protocol messages
- Encode and decode messages using JSON
- Identify message types such as `HANDSHAKE` and `MESSAGE`

The next development goal is to implement **basic file transfer between peers**.

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

### Run

```bash
go run ./cmd/peer
```

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
