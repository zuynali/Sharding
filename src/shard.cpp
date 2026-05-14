#include "proto.hpp"
#include "wal.hpp"

#include <arpa/inet.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unordered_map>
#include <unistd.h>
#include <vector>

// =============================================================================
// shard.cpp  —  Shard server process (Phase 1 + Phase 2)
// =============================================================================
//
// Usage:
//   ./shardkv --shard <id> --port <port> --data <dir>
//
// Example:
//   ./shardkv --shard 1 --port 7001 --data ./d1
//
// The shard is a single-threaded TCP server that:
//   1. Replays its WAL on startup to rebuild the in-memory KV store.
//   2. Accepts exactly ONE connection from the coordinator (not from clients).
//   3. Handles binary-framed messages: PUT_ONE, GET_ONE, DEL_ONE (Phase 1).
//   4. Handles STAGE, PREPARE, COMMIT_TXN, ABORT_TXN (Phase 2 — 2PC).
//   5. For every write: WAL first, fsync, then update memory, then respond.
//
// Single-threaded is deliberate — one coordinator connection, no concurrency
// needed.  Transactions are serialised per-shard by the coordinator's
// sequential 2PC messages.
// =============================================================================

// =============================================================================
// Global state
// =============================================================================

static std::unordered_map<std::string, std::string> g_store;
static WAL g_wal;
static int g_shard_id = 0;

// ── Phase 2: Staging area ────────────────────────────────────────────────────
// Maps tx_id → list of operations waiting for COMMIT.
// Populated by STAGE messages, resolved by COMMIT_TXN or ABORT_TXN.
static std::unordered_map<uint32_t, std::vector<StagedOp>> g_staged;

// =============================================================================
// Phase 1: Message handlers (single-key operations, no transaction)
// =============================================================================

static void handle_put_one(int fd, const MsgHeader &hdr,
                            const std::vector<uint8_t> &body)
{
    std::string key, val;
    try {
        parse_put_one(body, key, val);
    } catch (const std::exception &e) {
        send_msg(fd, PUT_ONE_RESP, hdr.tx_id, build_resp_err(e.what()));
        return;
    }

    // WAL before memory — if we crash after WAL but before updating memory,
    // replay will restore the write.  If we crash before WAL, it never happened.
    g_wal.append_single_put(key, val);   // writes + fsyncs
    g_store[key] = val;

    send_msg(fd, PUT_ONE_RESP, hdr.tx_id, build_resp_ok());
}

static void handle_get_one(int fd, const MsgHeader &hdr,
                            const std::vector<uint8_t> &body)
{
    std::string key;
    try {
        parse_get_one(body, key);
    } catch (const std::exception &e) {
        send_msg(fd, GET_ONE_RESP, hdr.tx_id, build_resp_err(e.what()));
        return;
    }

    auto it = g_store.find(key);
    if (it == g_store.end()) {
        send_msg(fd, GET_ONE_RESP, hdr.tx_id, build_get_resp(false, ""));
    } else {
        send_msg(fd, GET_ONE_RESP, hdr.tx_id, build_get_resp(true, it->second));
    }
    // GET does not touch the WAL — reads are never logged.
}

static void handle_del_one(int fd, const MsgHeader &hdr,
                            const std::vector<uint8_t> &body)
{
    std::string key;
    try {
        parse_get_one(body, key);   // DEL body has the same layout as GET
    } catch (const std::exception &e) {
        send_msg(fd, DEL_ONE_RESP, hdr.tx_id, build_resp_err(e.what()));
        return;
    }

    g_wal.append_single_del(key);
    g_store.erase(key);

    send_msg(fd, DEL_ONE_RESP, hdr.tx_id, build_resp_ok());
}

// =============================================================================
// Phase 2: Transaction message handlers (2PC)
// =============================================================================

// ── STAGE ────────────────────────────────────────────────────────────────────
// The coordinator sends STAGE to save an operation without applying it yet.
// Body layout: [1 byte op_type][key as length-prefixed string]
//              [if PUT: val as length-prefixed string]

static void handle_stage(int fd, const MsgHeader &hdr,
                          const std::vector<uint8_t> &body)
{
    if (body.empty()) {
        send_msg(fd, STAGE_RESP, hdr.tx_id, build_resp_err("empty STAGE body"));
        return;
    }

    uint8_t op_type = body[0];
    size_t offset = 1;

    std::string key, val;
    try {
        key = decode_str(body, offset);
        if (op_type == 1) { // PUT
            val = decode_str(body, offset);
        }
    } catch (const std::exception &e) {
        send_msg(fd, STAGE_RESP, hdr.tx_id, build_resp_err(e.what()));
        return;
    }

    // Save into staging area — do NOT touch g_store yet
    g_staged[hdr.tx_id].push_back({op_type, key, val});

    printf("[shard %d] staged %s %s for tx %u\n",
           g_shard_id,
           op_type == 1 ? "PUT" : "DEL",
           key.c_str(),
           hdr.tx_id);

    send_msg(fd, STAGE_RESP, hdr.tx_id, build_resp_ok());
}

// ── PREPARE ──────────────────────────────────────────────────────────────────
// PREPARE means: "are you ready to commit tx_id? Write your promise to WAL."
// After WAL::append_txn_prepare fsyncs, we MUST honour the decision —
// even if we crash and restart.

static void handle_prepare(int fd, const MsgHeader &hdr,
                            const std::vector<uint8_t> &/*body*/)
{
    uint32_t tx_id = hdr.tx_id;
    auto it = g_staged.find(tx_id);

    if (it == g_staged.end()) {
        // We have no staged ops for this tx — protocol error
        printf("[shard %d] PREPARE for unknown tx %u — voting NO\n",
               g_shard_id, tx_id);
        std::vector<uint8_t> resp = { 0 };  // 0 = NO
        send_msg(fd, PREPARE_RESP, tx_id, resp);
        return;
    }

    const char* force_abort = getenv("SHARD_FORCE_NO_PREPARE");
    if (force_abort && strcmp(force_abort, "1") == 0) {
        printf("[shard %d] SHARD_FORCE_NO_PREPARE is set — deliberately voting NO for tx %u\n",
               g_shard_id, tx_id);
        std::vector<uint8_t> resp = { 0 };  // 0 = NO
        send_msg(fd, PREPARE_RESP, tx_id, resp);
        return;
    }

    // Write a PREPARE record to WAL — this is the "promise"
    g_wal.append_txn_prepare(tx_id, it->second);

    printf("[shard %d] PREPARED tx %u (%zu ops) — voting YES\n",
           g_shard_id, tx_id, it->second.size());

    // Vote YES
    std::vector<uint8_t> resp = { 1 };  // 1 = YES
    send_msg(fd, PREPARE_RESP, tx_id, resp);
}

// ── COMMIT_TXN ───────────────────────────────────────────────────────────────
// COMMIT means: "apply those staged ops for real now."

static void handle_commit_txn(int fd, const MsgHeader &hdr,
                               const std::vector<uint8_t> &/*body*/)
{
    uint32_t tx_id = hdr.tx_id;
    auto it = g_staged.find(tx_id);

    if (it != g_staged.end()) {
        // Apply every staged operation to the main store
        for (const auto &op : it->second) {
            if (op.op_type == 1) {      // PUT
                g_store[op.key] = op.val;
            } else if (op.op_type == 2) { // DEL
                g_store.erase(op.key);
            }
        }
        // Write COMMIT to WAL, then clean up staging
        g_wal.append_txn_commit(tx_id);
        g_staged.erase(it);

        printf("[shard %d] COMMITTED tx %u\n", g_shard_id, tx_id);
    } else {
        // tx_id not found: already committed (duplicate message), just ACK
        printf("[shard %d] COMMIT for already-resolved tx %u — re-ACK\n",
               g_shard_id, tx_id);
    }

    std::vector<uint8_t> resp = { 1 };  // ACK
    send_msg(fd, COMMIT_RESP, tx_id, resp);
}

// ── ABORT_TXN ────────────────────────────────────────────────────────────────
// ABORT means: "throw away the staged ops, nothing happened."

static void handle_abort_txn(int fd, const MsgHeader &hdr,
                              const std::vector<uint8_t> &/*body*/)
{
    uint32_t tx_id = hdr.tx_id;
    auto it = g_staged.find(tx_id);

    if (it != g_staged.end()) {
        g_wal.append_txn_abort(tx_id);
        g_staged.erase(it);
        printf("[shard %d] ABORTED tx %u\n", g_shard_id, tx_id);
    } else {
        printf("[shard %d] ABORT for already-resolved tx %u — re-ACK\n",
               g_shard_id, tx_id);
    }

    std::vector<uint8_t> resp = { 1 };  // ACK
    send_msg(fd, ABORT_RESP, tx_id, resp);
}

// =============================================================================
// Connection loop — process messages from one coordinator connection
// =============================================================================

static void run_connection(int conn_fd) {
    printf("[shard %d] coordinator connected\n", g_shard_id);

    MsgHeader hdr{};
    std::vector<uint8_t> body;

    while (recv_msg(conn_fd, hdr, body)) {
        switch (static_cast<MsgType>(hdr.msg_type)) {
            // Phase 1: single-key ops
            case PUT_ONE: handle_put_one(conn_fd, hdr, body); break;
            case GET_ONE: handle_get_one(conn_fd, hdr, body); break;
            case DEL_ONE: handle_del_one(conn_fd, hdr, body); break;

            // Phase 2: 2PC transaction ops
            case STAGE:       handle_stage(conn_fd, hdr, body);       break;
            case PREPARE:     handle_prepare(conn_fd, hdr, body);     break;
            case COMMIT_TXN:  handle_commit_txn(conn_fd, hdr, body);  break;
            case ABORT_TXN:   handle_abort_txn(conn_fd, hdr, body);   break;

            default:
                fprintf(stderr, "[shard %d] unknown msg_type=%u, ignoring\n",
                        g_shard_id, hdr.msg_type);
                break;
        }
    }

    printf("[shard %d] coordinator disconnected\n", g_shard_id);
}

// =============================================================================
// Argument parsing
// =============================================================================

struct Args {
    int         shard_id = 0;
    int         port     = 0;
    std::string data_dir;
};

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --shard <id> --port <port> --data <dir>\n"
            "Example: %s --shard 1 --port 7001 --data ./d1\n",
            prog, prog);
    exit(1);
}

static Args parse_args(int argc, char *argv[]) {
    Args a;
    for (int i = 1; i < argc - 1; i++) {
        if      (std::string(argv[i]) == "--shard") a.shard_id = std::atoi(argv[++i]);
        else if (std::string(argv[i]) == "--port")  a.port     = std::atoi(argv[++i]);
        else if (std::string(argv[i]) == "--data")  a.data_dir = argv[++i];
    }
    if (a.shard_id <= 0 || a.port <= 0 || a.data_dir.empty()) usage(argv[0]);
    return a;
}

// =============================================================================
// main
// =============================================================================

int main(int argc, char *argv[]) {
    Args args = parse_args(argc, argv);
    g_shard_id = args.shard_id;

    // ── 1. WAL replay ──────────────────────────────────────────────────────
    // Replay before opening the WAL for writing — replay opens read-only.
    std::string wal_path = args.data_dir + "/shard.wal";

    // Create data dir if needed
    std::string mkdir_cmd = "mkdir -p " + args.data_dir;
    if (system(mkdir_cmd.c_str()) != 0) {
        fprintf(stderr, "[shard %d] failed to create data dir %s\n",
                g_shard_id, args.data_dir.c_str());
        return 1;
    }

    int replayed = 0;
    try {
        // Phase 2 replay: also rebuilds g_staged for "in-doubt" transactions
        replayed = WAL::replay(wal_path, g_store, g_staged);
    } catch (const std::exception &e) {
        fprintf(stderr, "[shard %d] WAL replay failed: %s\n",
                g_shard_id, e.what());
        return 1;
    }
    printf("[shard %d] WAL replay: %d records, %zu keys in store",
           g_shard_id, replayed, g_store.size());
    if (!g_staged.empty()) {
        printf(", %zu in-doubt txns restored", g_staged.size());
    }
    printf("\n");

    // ── 2. Open WAL for appending ──────────────────────────────────────────
    try {
        g_wal.open(wal_path);
    } catch (const std::exception &e) {
        fprintf(stderr, "[shard %d] WAL open failed: %s\n",
                g_shard_id, e.what());
        return 1;
    }

    // ── 3. Create TCP listener ─────────────────────────────────────────────
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }

    // SO_REUSEADDR lets us restart quickly without "address already in use"
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(args.port));

    if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listen_fd, 4) < 0) { perror("listen"); return 1; }

    printf("[shard %d] ready on port %d\n", g_shard_id, args.port);

    // ── 4. Accept loop ─────────────────────────────────────────────────────
    // Accept one connection at a time (coordinator only).
    // If the coordinator disconnects and reconnects, we accept again.
    while (true) {
        sockaddr_in peer{};
        socklen_t   peer_len = sizeof(peer);
        int conn_fd = accept(listen_fd,
                             reinterpret_cast<sockaddr *>(&peer), &peer_len);
        if (conn_fd < 0) { perror("accept"); continue; }

        run_connection(conn_fd);
        close(conn_fd);
    }

    close(listen_fd);
    return 0;
}