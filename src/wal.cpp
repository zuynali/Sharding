#include "wal.hpp"

#include <arpa/inet.h>     // htonl, ntohl
#include <cstring>
#include <fcntl.h>         // open, O_WRONLY, O_CREAT, O_APPEND
#include <stdexcept>
#include <unistd.h>        // write, fsync, close, read

// =============================================================================
// CRC-32  (ISO 3309 / Ethernet polynomial 0xEDB88320)
// =============================================================================
// Think of CRC32 like a fingerprint of the data.  If even one byte flips
// on disk (power cut mid-write, bad sector), the fingerprint won't match
// on replay and we'll refuse to apply the corrupted record.
// =============================================================================

static uint32_t crc32_table[256];
static bool     crc32_table_ready = false;

static void init_crc32_table() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_table_ready = true;
}

static uint32_t crc32(const uint8_t *data, size_t len) {
    if (!crc32_table_ready) init_crc32_table();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        c = crc32_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

// =============================================================================
// Encode / decode helpers (network byte order)
// =============================================================================

static void put_u32(std::vector<uint8_t> &buf, uint32_t v) {
    uint32_t net = htonl(v);
    const uint8_t *p = reinterpret_cast<const uint8_t *>(&net);
    buf.insert(buf.end(), p, p + 4);
}

static void put_str(std::vector<uint8_t> &buf, const std::string &s) {
    put_u32(buf, static_cast<uint32_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

// Read a big-endian uint32 from raw bytes at offset, advance offset.
static uint32_t get_u32(const uint8_t *data, size_t total, size_t &off) {
    if (off + 4 > total)
        throw std::runtime_error("WAL replay: truncated uint32 field");
    uint32_t net;
    std::memcpy(&net, data + off, 4);
    off += 4;
    return ntohl(net);
}

// Read a length-prefixed string, advance offset.
static std::string get_str(const uint8_t *data, size_t total, size_t &off) {
    uint32_t len = get_u32(data, total, off);
    if (off + len > total)
        throw std::runtime_error("WAL replay: truncated string field");
    std::string s(reinterpret_cast<const char *>(data + off), len);
    off += len;
    return s;
}

// =============================================================================
// WAL::build_record
// =============================================================================
// Serialises one record into:
//   [1 byte type][4 byte payload_len][4 byte crc32][payload...]
//
// The CRC covers: type byte + raw payload_len bytes + payload bytes.
// This means a flipped bit in *any* field (including the length field itself)
// will be caught on replay.
// =============================================================================

std::vector<uint8_t> WAL::build_record(WalRecType type,
                                        const std::vector<uint8_t> &payload)
{
    // Build the portion that CRC covers: [type(1)][payload_len(4)][payload]
    uint32_t payload_len = static_cast<uint32_t>(payload.size());

    std::vector<uint8_t> crc_input;
    crc_input.reserve(1 + 4 + payload.size());
    crc_input.push_back(static_cast<uint8_t>(type));
    put_u32(crc_input, payload_len);
    crc_input.insert(crc_input.end(), payload.begin(), payload.end());

    uint32_t checksum = crc32(crc_input.data(), crc_input.size());

    // Assemble: [type][payload_len][crc32][payload]
    std::vector<uint8_t> record;
    record.reserve(WAL_HDR_SIZE + payload.size());
    record.push_back(static_cast<uint8_t>(type));
    put_u32(record, payload_len);
    put_u32(record, checksum);
    record.insert(record.end(), payload.begin(), payload.end());

    return record;
}

// =============================================================================
// WAL::open
// =============================================================================

void WAL::open(const std::string &path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0)
        throw std::runtime_error("WAL::open failed: " + path);
}

WAL::~WAL() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
    }
}

// =============================================================================
// WAL::write_and_sync
// =============================================================================

void WAL::write_and_sync(const void *buf, size_t len) {
    const char *p    = static_cast<const char *>(buf);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t w = ::write(fd_, p, remaining);
        if (w <= 0)
            throw std::runtime_error("WAL write failed");
        p         += w;
        remaining -= static_cast<size_t>(w);
    }
    // fsync is mandatory — without it the "durability" argument for 2PC breaks.
    // Think of fsync as sealing the envelope before it leaves your hands:
    // until you seal it, a power cut means the letter never existed.
    if (::fsync(fd_) != 0)
        throw std::runtime_error("WAL fsync failed");
}

// =============================================================================
// WAL::append_single_put
// =============================================================================

void WAL::append_single_put(const std::string &key, const std::string &val) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");

    std::vector<uint8_t> payload;
    payload.reserve(8 + key.size() + val.size());
    put_str(payload, key);
    put_str(payload, val);

    auto record = build_record(WalRecType::SINGLE_PUT, payload);
    write_and_sync(record.data(), record.size());
}

// =============================================================================
// WAL::append_single_del
// =============================================================================

void WAL::append_single_del(const std::string &key) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");

    std::vector<uint8_t> payload;
    payload.reserve(4 + key.size());
    put_str(payload, key);

    auto record = build_record(WalRecType::SINGLE_DEL, payload);
    write_and_sync(record.data(), record.size());
}

// =============================================================================
// Phase 2: WAL::append_txn_prepare
// =============================================================================
// Called during PREPARE: write the full list of staged ops to disk.
// After this fsync, the shard has PROMISED to commit if the coordinator
// says so — even across crashes.
// Payload: [tx_id: 4][op_count: 4][op1][op2]...
// Each op:  [op_type: 1][key: len-prefixed][val: len-prefixed if PUT]
// =============================================================================

void WAL::append_txn_prepare(uint32_t tx_id,
                              const std::vector<StagedOp> &ops)
{
    if (fd_ < 0) throw std::runtime_error("WAL not open");

    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    put_u32(payload, static_cast<uint32_t>(ops.size()));
    for (const auto &op : ops) {
        payload.push_back(op.op_type);
        put_str(payload, op.key);
        if (op.op_type == 1) {  // PUT
            put_str(payload, op.val);
        }
    }

    auto record = build_record(WalRecType::TXN_PREPARE, payload);
    write_and_sync(record.data(), record.size());
}

// =============================================================================
// Phase 2: WAL::append_txn_commit
// =============================================================================
// Called after applying ops: record that this tx committed.
// Payload: [tx_id: 4]
// =============================================================================

void WAL::append_txn_commit(uint32_t tx_id) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");
    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    auto record = build_record(WalRecType::TXN_COMMIT, payload);
    write_and_sync(record.data(), record.size());
}

// =============================================================================
// Phase 2: WAL::append_txn_abort
// =============================================================================
// Called on abort: record that this tx was discarded.
// Payload: [tx_id: 4]
// =============================================================================

void WAL::append_txn_abort(uint32_t tx_id) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");
    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    auto record = build_record(WalRecType::TXN_ABORT, payload);
    write_and_sync(record.data(), record.size());
}

// =============================================================================
// Phase 2 Coordinator: WAL records
// =============================================================================

void WAL::append_coord_begin(uint32_t tx_id) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");
    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    auto record = build_record(WalRecType::TX_BEGIN, payload);
    write_and_sync(record.data(), record.size());
}

void WAL::append_coord_commit_decision(uint32_t tx_id, const std::set<int> &shards) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");
    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    put_u32(payload, static_cast<uint32_t>(shards.size()));
    for (int sid : shards) put_u32(payload, sid);
    auto record = build_record(WalRecType::TX_COMMIT_DECISION, payload);
    write_and_sync(record.data(), record.size());
}

void WAL::append_coord_abort_decision(uint32_t tx_id, const std::set<int> &shards) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");
    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    put_u32(payload, static_cast<uint32_t>(shards.size()));
    for (int sid : shards) put_u32(payload, sid);
    auto record = build_record(WalRecType::TX_ABORT_DECISION, payload);
    write_and_sync(record.data(), record.size());
}

void WAL::append_coord_done(uint32_t tx_id) {
    if (fd_ < 0) throw std::runtime_error("WAL not open");
    std::vector<uint8_t> payload;
    put_u32(payload, tx_id);
    auto record = build_record(WalRecType::TX_DONE, payload);
    write_and_sync(record.data(), record.size());
}

int WAL::replay_coord(const std::string &path,
                      std::unordered_map<uint32_t, CoordDecision> &pending_out,
                      uint32_t &max_tx_id_out)
{
    max_tx_id_out = 0;
    int rfd = ::open(path.c_str(), O_RDONLY);
    if (rfd < 0) return 0;

    std::vector<uint8_t> buf;
    {
        uint8_t tmp[4096];
        ssize_t n;
        while ((n = ::read(rfd, tmp, sizeof(tmp))) > 0)
            buf.insert(buf.end(), tmp, tmp + n);
        ::close(rfd);
    }

    size_t pos      = 0;
    int    replayed = 0;
    size_t total    = buf.size();

    while (pos < total) {
        if (pos + WAL_HDR_SIZE > total) break;

        uint8_t  rec_type_byte = buf[pos];
        uint32_t payload_len;
        uint32_t stored_crc;
        {
            size_t off = pos + 1;
            uint32_t net_len; std::memcpy(&net_len, buf.data() + off, 4);
            payload_len = ntohl(net_len); off += 4;
            uint32_t net_crc; std::memcpy(&net_crc, buf.data() + off, 4);
            stored_crc = ntohl(net_crc);
        }

        if (pos + WAL_HDR_SIZE + payload_len > total) break;

        std::vector<uint8_t> crc_input;
        crc_input.reserve(1 + 4 + payload_len);
        crc_input.push_back(rec_type_byte);
        {
            uint32_t net_len = htonl(payload_len);
            const uint8_t *p = reinterpret_cast<const uint8_t *>(&net_len);
            crc_input.insert(crc_input.end(), p, p + 4);
        }
        const uint8_t *payload_ptr = buf.data() + pos + WAL_HDR_SIZE;
        crc_input.insert(crc_input.end(), payload_ptr, payload_ptr + payload_len);

        if (crc32(crc_input.data(), crc_input.size()) != stored_crc)
            throw std::runtime_error("WAL replay: CRC mismatch in coord WAL");

        size_t off = 0;
        auto   type = static_cast<WalRecType>(rec_type_byte);

        // All coordinator records start with tx_id
        uint32_t tx_id = get_u32(payload_ptr, payload_len, off);
        if (tx_id > max_tx_id_out) max_tx_id_out = tx_id;

        switch (type) {
            case WalRecType::TX_BEGIN:
                // No action needed for pending_out
                break;
            case WalRecType::TX_COMMIT_DECISION: {
                uint32_t count = get_u32(payload_ptr, payload_len, off);
                CoordDecision dec;
                dec.commit = true;
                for (uint32_t i = 0; i < count; i++) {
                    dec.shards.insert(get_u32(payload_ptr, payload_len, off));
                }
                pending_out[tx_id] = dec;
                break;
            }
            case WalRecType::TX_ABORT_DECISION: {
                uint32_t count = get_u32(payload_ptr, payload_len, off);
                CoordDecision dec;
                dec.commit = false;
                for (uint32_t i = 0; i < count; i++) {
                    dec.shards.insert(get_u32(payload_ptr, payload_len, off));
                }
                pending_out[tx_id] = dec;
                break;
            }
            case WalRecType::TX_DONE: {
                pending_out.erase(tx_id);
                break;
            }
            default:
                break;
        }

        pos += WAL_HDR_SIZE + payload_len;
        replayed++;
    }
    return replayed;
}

// =============================================================================
// WAL::replay  (Phase 2 version — handles TXN_PREPARE/COMMIT/ABORT)
// =============================================================================
// Read the entire WAL file into memory, then walk it record-by-record.
// Apply each valid record to store_out.  Throw on CRC mismatch (corruption).
//
// Phase 2 logic:
//   - TXN_PREPARE → save ops into a local pending_prepares map
//   - TXN_COMMIT  → apply the ops from pending_prepares to store_out
//   - TXN_ABORT   → discard from pending_prepares
//   - After the loop, anything left in pending_prepares is "in doubt" —
//     move it to staged_out so the shard can wait for the coordinator.
// =============================================================================

int WAL::replay(const std::string &path,
                std::unordered_map<std::string, std::string> &store_out,
                std::unordered_map<uint32_t, std::vector<StagedOp>> &staged_out)
{
    // Open for reading
    int rfd = ::open(path.c_str(), O_RDONLY);
    if (rfd < 0) return 0;   // file doesn't exist yet — fresh shard

    // Read entire file into memory (WAL files are small in this project)
    std::vector<uint8_t> buf;
    {
        uint8_t tmp[4096];
        ssize_t n;
        while ((n = ::read(rfd, tmp, sizeof(tmp))) > 0)
            buf.insert(buf.end(), tmp, tmp + n);
        ::close(rfd);
    }

    // Local map for PREPARE records that haven't been committed/aborted yet
    std::unordered_map<uint32_t, std::vector<StagedOp>> pending_prepares;

    size_t pos      = 0;
    int    replayed = 0;
    size_t total    = buf.size();

    while (pos < total) {
        // ── 1. Need at least WAL_HDR_SIZE bytes for the header ──
        if (pos + WAL_HDR_SIZE > total) {
            // Truncated header at end of file — crash before fsync, ignore.
            break;
        }

        // ── 2. Parse header ──
        uint8_t  rec_type_byte = buf[pos];
        uint32_t payload_len;
        uint32_t stored_crc;
        {
            size_t off = pos + 1;
            // Read payload_len (4 bytes, big-endian)
            uint32_t net_len; std::memcpy(&net_len, buf.data() + off, 4);
            payload_len = ntohl(net_len); off += 4;
            // Read stored crc (4 bytes, big-endian)
            uint32_t net_crc; std::memcpy(&net_crc, buf.data() + off, 4);
            stored_crc = ntohl(net_crc);
        }

        // ── 3. Check payload is all there ──
        if (pos + WAL_HDR_SIZE + payload_len > total) {
            // Truncated payload — crash before fsync, ignore.
            break;
        }

        // ── 4. Verify CRC ──
        // CRC covers: [type(1)] + [payload_len(4)] + [payload]
        std::vector<uint8_t> crc_input;
        crc_input.reserve(1 + 4 + payload_len);
        crc_input.push_back(rec_type_byte);
        {
            uint32_t net_len = htonl(payload_len);
            const uint8_t *p = reinterpret_cast<const uint8_t *>(&net_len);
            crc_input.insert(crc_input.end(), p, p + 4);
        }
        const uint8_t *payload_ptr = buf.data() + pos + WAL_HDR_SIZE;
        crc_input.insert(crc_input.end(), payload_ptr, payload_ptr + payload_len);

        uint32_t computed_crc = crc32(crc_input.data(), crc_input.size());
        if (computed_crc != stored_crc)
            throw std::runtime_error("WAL replay: CRC mismatch at offset "
                                     + std::to_string(pos)
                                     + " — log is corrupted");

        // ── 5. Apply the record ──
        size_t off = 0;   // offset within payload
        auto   type = static_cast<WalRecType>(rec_type_byte);

        switch (type) {
            case WalRecType::SINGLE_PUT: {
                std::string key = get_str(payload_ptr, payload_len, off);
                std::string val = get_str(payload_ptr, payload_len, off);
                store_out[key] = val;
                break;
            }
            case WalRecType::SINGLE_DEL: {
                std::string key = get_str(payload_ptr, payload_len, off);
                store_out.erase(key);
                break;
            }

            // ── Phase 2 record types ──

            case WalRecType::TXN_PREPARE: {
                // Rebuild the staging area — we were prepared but don't
                // know yet if committed
                uint32_t tx_id    = get_u32(payload_ptr, payload_len, off);
                uint32_t op_count = get_u32(payload_ptr, payload_len, off);
                std::vector<StagedOp> ops;
                for (uint32_t i = 0; i < op_count; i++) {
                    StagedOp op;
                    if (off >= payload_len)
                        throw std::runtime_error("WAL replay: truncated TXN_PREPARE op");
                    op.op_type = payload_ptr[off++];
                    op.key = get_str(payload_ptr, payload_len, off);
                    if (op.op_type == 1) {  // PUT
                        op.val = get_str(payload_ptr, payload_len, off);
                    }
                    ops.push_back(op);
                }
                // Tentatively add — will be resolved by COMMIT or ABORT
                pending_prepares[tx_id] = std::move(ops);
                break;
            }
            case WalRecType::TXN_COMMIT: {
                uint32_t tx_id = get_u32(payload_ptr, payload_len, off);
                auto it = pending_prepares.find(tx_id);
                if (it != pending_prepares.end()) {
                    // Apply the staged ops to the store
                    for (const auto &op : it->second) {
                        if (op.op_type == 1)      // PUT
                            store_out[op.key] = op.val;
                        else if (op.op_type == 2) // DEL
                            store_out.erase(op.key);
                    }
                    pending_prepares.erase(it);
                }
                break;
            }
            case WalRecType::TXN_ABORT: {
                uint32_t tx_id = get_u32(payload_ptr, payload_len, off);
                pending_prepares.erase(tx_id);  // just discard
                break;
            }

            default:
                throw std::runtime_error("WAL replay: unknown record type "
                                         + std::to_string(rec_type_byte));
        }

        pos += WAL_HDR_SIZE + payload_len;
        replayed++;
    }

    // Any tx_id left in pending_prepares is "in doubt" — the shard prepared
    // but never got a COMMIT or ABORT.  Restore these to staged_out so the
    // shard can hold them until the coordinator reconnects and re-sends
    // the decision.
    for (auto &[tx_id, ops] : pending_prepares) {
        staged_out[tx_id] = std::move(ops);
    }

    return replayed;
}

// =============================================================================
// Backward-compatible overload (Phase 1 callers)
// =============================================================================

int WAL::replay(const std::string &path,
                std::unordered_map<std::string, std::string> &store_out)
{
    std::unordered_map<uint32_t, std::vector<StagedOp>> staged_discard;
    return replay(path, store_out, staged_discard);
}