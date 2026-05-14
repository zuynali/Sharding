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
// Phase 2 record types:
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
#include <set>

// =============================================================================
// Record type tag  (1 byte on disk)
// =============================================================================

enum class WalRecType : uint8_t {
    SINGLE_PUT  = 1,
    SINGLE_DEL  = 2,

    // Phase 2 Shard
    TXN_PREPARE = 10,
    TXN_COMMIT  = 11,
    TXN_ABORT   = 12,

    // Phase 2 Coordinator
    TX_BEGIN           = 20,
    TX_COMMIT_DECISION = 21,
    TX_ABORT_DECISION  = 22,
    TX_DONE            = 23,
};

// =============================================================================
// WAL header constants
// =============================================================================

static constexpr size_t WAL_HDR_SIZE = 9;   // 1 (type) + 4 (len) + 4 (crc)

// =============================================================================
// Staged operation  —  used by both shard and WAL
// =============================================================================
// Represents a single operation that has been staged inside a transaction
// but not yet applied to the main store.

struct StagedOp {
    uint8_t     op_type;  // 1 = PUT, 2 = DEL
    std::string key;
    std::string val;      // empty for DEL
};

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

    // ── Phase 2 WAL records ──────────────────────────────────────────────

    // Write a TXN_PREPARE record: the shard promises it can commit these ops.
    void append_txn_prepare(uint32_t tx_id,
                            const std::vector<StagedOp> &ops);

    // Write a TXN_COMMIT record: the ops for tx_id have been applied.
    void append_txn_commit(uint32_t tx_id);

    // Write a TXN_ABORT record: the ops for tx_id were discarded.
    void append_txn_abort(uint32_t tx_id);

    // ── Phase 2 Coordinator WAL records ──────────────────────────────────

    void append_coord_begin(uint32_t tx_id);
    void append_coord_commit_decision(uint32_t tx_id, const std::set<int> &shards);
    void append_coord_abort_decision(uint32_t tx_id, const std::set<int> &shards);
    void append_coord_done(uint32_t tx_id);

    struct CoordDecision {
        bool commit;
        std::set<int> shards;
    };

    // Replay the coordinator WAL. Returns the number of records replayed.
    // Populates pending_out with transactions that have a decision but no TX_DONE.
    // Also outputs the maximum tx_id seen to max_tx_id_out.
    static int replay_coord(const std::string &path,
                            std::unordered_map<uint32_t, CoordDecision> &pending_out,
                            std::set<uint32_t> &pending_begins_out,
                            uint32_t &max_tx_id_out);

    // Replay the WAL from disk into store_out.
    // Also populates staged_out with any "in-doubt" transactions
    // (prepared but neither committed nor aborted — crash recovery).
    // Call this once at shard startup *before* open() to rebuild state.
    // Returns the number of records replayed.
    // Throws std::runtime_error on a corrupted (bad-CRC) record.
    static int replay(const std::string &path,
                      std::unordered_map<std::string, std::string> &store_out,
                      std::unordered_map<uint32_t, std::vector<StagedOp>> &staged_out);

    // Backward-compatible overload (Phase 1 callers that don't care about staging)
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