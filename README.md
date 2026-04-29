# Sharded KV Store with Two-Phase Commit

**Advanced Database Management — CS 4th Semester | Project 02**  
Language: C++17

---

## Overview

A distributed key-value store partitioned across three shard servers, with a coordinator that routes client requests using **consistent hashing** (FNV-1a, 150 virtual nodes per shard). Multi-key transactions spanning multiple shards use **two-phase commit (2PC)** to guarantee atomicity.

---

## Project Structure

```
.
├── src/
│   ├── ring.hpp       # Consistent hash ring (header-only)
│   ├── proto.hpp      # Binary wire protocol (header-only)
│   ├── wal.hpp        # Write-ahead log interface
│   ├── wal.cpp        # WAL implementation
│   ├── shard.cpp      # Shard server binary
│   ├── coord.cpp      # Coordinator binary
│   └── cli.cpp        # Client REPL binary
├── tests/
│   └── ring_test.cpp  # Unit tests for consistent hash ring
├── Makefile
├── docker-compose.yml
└── README.md
```

---

## Build Instructions

### Prerequisites

- **OS:** Ubuntu 22.04 LTS or 24.04 LTS (or any recent Linux)
- **Compiler:** `g++` with C++17 support (`sudo apt install build-essential`)

### Build All Binaries

```bash
make
```

This produces four binaries in `bin/`:

| Binary | Description |
|---|---|
| `bin/shardkv` | Shard server |
| `bin/shardkv-coord` | Coordinator |
| `bin/shardkv-cli` | Client REPL |
| `bin/ring_test` | Ring unit tests |

### Run Unit Tests

```bash
make test
```

Expected output: all ring tests pass (distribution uniformity, minimal key movement on resize, edge cases).

### Clean

```bash
make clean
```

---

## Deployment Options

### Option 1 — Four Processes on One Machine (Recommended for Development)

Open five terminals (or use `tmux`):

```bash
# Terminal 1 — Shard 1
./bin/shardkv --shard 1 --port 7001 --data ./d1

# Terminal 2 — Shard 2
./bin/shardkv --shard 2 --port 7002 --data ./d2

# Terminal 3 — Shard 3
./bin/shardkv --shard 3 --port 7003 --data ./d3

# Terminal 4 — Coordinator
./bin/shardkv-coord --port 6001 \
  --shards 1@localhost:7001,2@localhost:7002,3@localhost:7003

# Terminal 5 — Client
./bin/shardkv-cli localhost 6001
```

### Option 2 — Docker Compose

```bash
docker-compose up --build
```

Then connect a client:

```bash
./bin/shardkv-cli localhost 6001
```

To simulate a shard failure for chaos testing:

```bash
docker-compose stop shard1
```

Restart it:

```bash
docker-compose start shard1
```

### Option 3 — Separate Machines

Replace `localhost` with the actual IP of each machine:

```bash
# On machine hosting coordinator:
./bin/shardkv-coord --port 6001 \
  --shards 1@192.168.1.10:7001,2@192.168.1.11:7002,3@192.168.1.12:7003

# On each shard machine:
./bin/shardkv --shard <id> --port <port> --data ./data
```

---

## Supported Client Commands

Connect via: `./bin/shardkv-cli <host> <port>`

| Command | Description |
|---|---|
| `PUT <key> <value>` | Write a key-value pair |
| `GET <key>` | Read a key's value |
| `DELETE <key>` | Remove a key |
| `\keys` | List all tracked keys and their owning shards |
| `\stats` | Show cluster statistics (shards up/down, key counts, op counts) |
| `QUIT` | Disconnect |

### Example Session

```
$ ./bin/shardkv-cli localhost 6001
connected to coordinator at localhost:6001

> PUT alice 100
OK (shard 2)

> PUT bob 50
OK (shard 1)

> PUT carol 75
OK (shard 3)

> GET alice
100 (shard 2)

> \keys
alice -> shard 2
bob   -> shard 1
carol -> shard 3

> \stats
coordinator:     1
shards total:    3
shards up:       3
keys tracked:    3
puts:            3
gets:            1
deletes:         0
shard 1:  localhost:7001  UP  keys=[bob]
shard 2:  localhost:7002  UP  keys=[alice]
shard 3:  localhost:7003  UP  keys=[carol]

> DELETE bob
OK (shard 1)

> QUIT
BYE
```

---

## Architecture Notes

### Consistent Hashing

- Hash function: FNV-1a 32-bit (deterministic, non-cryptographic)
- Virtual nodes: 150 per shard → 450 ring positions for 3 shards
- Ring lookup: binary search, O(log 450) ≈ 9 comparisons
- Key distribution: each shard receives approximately 28–38% of keys

### Wire Protocol

**Client → Coordinator:** Plain text, newline-terminated lines.

**Coordinator → Shard:** Binary, 12-byte framed header:

```
Offset  Size  Field
  0      4    msg_len   (total length incl. header, uint32 big-endian)
  4      2    msg_type  (uint16 big-endian)
  6      2    flags     (uint16, reserved = 0)
  8      4    tx_id     (uint32, 0 for single-key ops)
 12     ...   body      (type-specific)
```

### WAL (Write-Ahead Log)

Each shard maintains an append-only WAL at `<data_dir>/shard.wal`. Records are CRC32-checked. On startup, the shard replays the WAL to rebuild in-memory state before accepting connections.

---

## Known Limitations (Phase 1)

- The coordinator is **single-threaded** — it handles one client connection at a time. Concurrent clients will queue.
- The `\keys` map is built lazily from coordinator-routed writes only. Keys written directly to a shard (e.g., pre-existing WAL data from a previous run) will not appear in `\keys` until the coordinator routes a new write for that key.
- No authentication or TLS. Intended for trusted local/development networks only.
- Phase 2 (2PC transactions) and Phase 3 (chaos testing) are not yet implemented.
