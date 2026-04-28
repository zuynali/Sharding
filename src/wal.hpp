#pragma once

// =============================================================================
// wal.hpp  —  Write-Ahead Log for the shard process
// =============================================================================
//
// The WAL is the shard's receipt book.  Before any data is changed in memory,
// a record is written to disk and fsynced.  If the process crashes mid-write,
// on restart the shard replays the WAL to rebuild exactly the state that was
// durably committed — no more, no less.
//
// Analogy: think of the WAL like a cashier's receipt roll.  Every transaction
// gets a receipt *before* the till is updated.  If the power cuts out, you
// rewind the receipt roll on restart and rebuild the till from scratch.
//
// On-disk record layout (all integers in big-endian / network byte order):
//
//   Offset  Size  Field
//   ------  ----  -------------------------------------------------
//     0      1    record_type   (WalRecType enum, 1 byte)
//     1      4    payload_len   (uint32, bytes after the 9-byte header)
//     5      4    crc32         (checksum of type + payload_len + payload)
//     9      ...  payload       (type-specific, see below)
//
// Phase 1 record types and their payloads:
//
//   SINGLE_PUT  payload: [4-byte klen][key bytes][4-byte vlen][val bytes]
//   SINGLE_DEL  payload: [4-byte klen][key bytes]
//
// Phase 2 record types (stubs — implemented in Phase 2):
//
//   TXN_PREPARE  payload: [4-byte tx_id][4-byte op_count][ops...]
//                   each op: [1-byte op_type][4-byte klen][key][4-byte vlen][val]
//   TXN_COMMIT   payload: [4-byte tx_id]
//   TXN_ABORT    payload: [4-byte tx_id]
//
// =============================================================================

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

// =============================================================================
// Record type tag  (1 byte on disk)
// =============================================================================

enum class WalRecType : uint8_t {
    SINGLE_PUT  = 1,
    SINGLE_DEL  = 2,

    // Phase 2 stubs
    TXN_PREPARE = 10,
    TXN_COMMIT  = 11,
    TXN_ABORT   = 12,
};

// =============================================================================
// WAL header constants
// =============================================================================

static constexpr size_t WAL_HDR_SIZE = 9;   // 1 (type) + 4 (len) + 4 (crc)

// =============================================================================
// Decoded WAL record  (in-memory representation after replay)
// =============================================================================

struct WalRecord {
    WalRecType  type;
    uint32_t    tx_id;      // 0 for SINGLE ops
    std::string key;
    std::string value;      // empty for DEL
};

// =============================================================================
// WAL class
// =============================================================================

class WAL {
public:
    WAL() = default;
    ~WAL();

    // Open (or create) the WAL file at path.
    // Throws std::runtime_error on failure.
    void open(const std::string &path);

    // Append a SINGLE_PUT record and fsync.
    void append_single_put(const std::string &key, const std::string &val);

    // Append a SINGLE_DEL record and fsync.
    void append_single_del(const std::string &key);

    // Replay the WAL from disk into store_out.
    // Call this once at shard startup *before* open() to rebuild state.
    // Returns the number of records replayed.
    // Throws std::runtime_error on a corrupted (bad-CRC) record.
    static int replay(const std::string &path,
                      std::unordered_map<std::string, std::string> &store_out);

    bool is_open() const { return fd_ >= 0; }

private:
    int fd_ = -1;   // file descriptor (O_WRONLY | O_CREAT | O_APPEND)

    // Write bytes to the WAL and fsync. Throws on I/O error.
    void write_and_sync(const void *buf, size_t len);

    // Build a serialised record: 9-byte header + payload.
    static std::vector<uint8_t> build_record(WalRecType type,
                                              const std::vector<uint8_t> &payload);
};