#include "proto.hpp"
#include "ring.hpp"
#include "wal.hpp"

#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <netdb.h>
#include <netinet/in.h>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <fcntl.h>

// =============================================================================
// coord.cpp  —  Coordinator process (Phase 1 + Phase 2: Two-Phase Commit)
// =============================================================================

struct ShardConn {
    int         id;
    std::string host;
    int         port;
    int         fd = -1;
};

// ── Phase 2: Transaction state ───────────────────────────────────────────────

struct CoordOp {
    uint8_t     op_type;  // 1=PUT, 2=DEL
    std::string key;
    std::string val;
    int         shard_id;
};

struct TxnState {
    int                  client_fd;
    std::vector<CoordOp> ops;
    std::set<int>        shards;   // shard IDs involved
};

// =============================================================================
// Global coordinator state
// =============================================================================

static Hashring                           g_ring;
static std::vector<ShardConn>            g_shards;
static std::map<int, ShardConn *>        g_shard_map;
static std::map<std::string, int>        g_key_shard;
static int                               g_coord_port = 0;

// Counters
static long g_puts = 0, g_gets = 0, g_dels = 0;

// Phase 2 state
static std::unordered_map<uint32_t, TxnState> g_txns;
static uint32_t g_next_tx_id = 1;
static WAL g_coord_wal;   // coordinator's own WAL for decisions

// =============================================================================
// Argument parsing
// =============================================================================

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --port <port> --shards <id@host:port,...>\n"
            "Example: %s --port 6001 "
            "--shards 1@localhost:7001,2@localhost:7002,3@localhost:7003\n",
            prog, prog);
    exit(1);
}

static std::vector<ShardConn> parse_shards(const std::string &spec) {
    std::vector<ShardConn> result;
    std::istringstream ss(spec);
    std::string token;
    while (std::getline(ss, token, ',')) {
        size_t at  = token.find('@');
        size_t col = token.rfind(':');
        if (at == std::string::npos || col == std::string::npos || col < at)
            throw std::invalid_argument("bad shard spec: " + token);
        ShardConn sc;
        sc.id   = std::stoi(token.substr(0, at));
        sc.host = token.substr(at + 1, col - at - 1);
        sc.port = std::stoi(token.substr(col + 1));
        result.push_back(sc);
    }
    if (result.empty()) throw std::invalid_argument("empty shard list");
    return result;
}

// =============================================================================
// TCP helpers
// =============================================================================

static int tcp_connect(const std::string &host, int port) {
    addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0) return -1;
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return -1; }
    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        close(fd); freeaddrinfo(res); return -1;
    }
    freeaddrinfo(res);
    return fd;
}

static void client_send(int fd, const std::string &s) {
    write_exact(fd, s.data(), s.size());
}

// =============================================================================
// Shard helpers
// =============================================================================

static ShardConn *find_shard(int id) {
    auto it = g_shard_map.find(id);
    return (it == g_shard_map.end()) ? nullptr : it->second;
}

static bool shard_put(int shard_id, const std::string &key,
                       const std::string &val, std::string &err_out) {
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) { err_out = "shard unavailable"; return false; }
    MsgHeader rh{}; std::vector<uint8_t> rb;
    try { roundtrip(sc->fd, PUT_ONE, 0, build_put_one(key, val), rh, rb); }
    catch (const std::exception &e) { err_out = e.what(); sc->fd = -1; return false; }
    return parse_simple_resp(rb, err_out);
}

static bool shard_get(int shard_id, const std::string &key,
                       std::string &val_out, std::string &err_out) {
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) { err_out = "shard unavailable"; return false; }
    MsgHeader rh{}; std::vector<uint8_t> rb;
    try { roundtrip(sc->fd, GET_ONE, 0, build_get_one(key), rh, rb); }
    catch (const std::exception &e) { err_out = e.what(); sc->fd = -1; return false; }
    if (!parse_get_resp(rb, val_out)) { err_out = "not found"; return false; }
    return true;
}

static bool shard_del(int shard_id, const std::string &key, std::string &err_out) {
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) { err_out = "shard unavailable"; return false; }
    MsgHeader rh{}; std::vector<uint8_t> rb;
    try { roundtrip(sc->fd, DEL_ONE, 0, build_del_one(key), rh, rb); }
    catch (const std::exception &e) { err_out = e.what(); sc->fd = -1; return false; }
    return parse_simple_resp(rb, err_out);
}

// =============================================================================
// Phase 1: Single-key command handlers
// =============================================================================

static void cmd_put(int cfd, const std::string &key, const std::string &val) {
    int sid = g_ring.lookup(key);
    std::string err;
    if (shard_put(sid, key, val, err)) {
        g_key_shard[key] = sid; g_puts++;
        client_send(cfd, "OK (shard " + std::to_string(sid) + ")\n");
    } else { client_send(cfd, "ERR " + err + "\n"); }
}

static void cmd_get(int cfd, const std::string &key) {
    int sid = g_ring.lookup(key);
    std::string val, err;
    if (shard_get(sid, key, val, err)) {
        g_gets++;
        client_send(cfd, val + " (shard " + std::to_string(sid) + ")\n");
    } else { client_send(cfd, "ERR " + err + "\n"); }
}

static void cmd_delete(int cfd, const std::string &key) {
    int sid = g_ring.lookup(key);
    std::string err;
    if (shard_del(sid, key, err)) {
        g_key_shard.erase(key); g_dels++;
        client_send(cfd, "OK (shard " + std::to_string(sid) + ")\n");
    } else { client_send(cfd, "ERR " + err + "\n"); }
}

static void cmd_keys(int cfd) {
    if (g_key_shard.empty()) { client_send(cfd, "(no keys)\n"); return; }
    for (auto &[key, sid] : g_key_shard)
        client_send(cfd, key + " -> shard " + std::to_string(sid) + "\n");
}

static void cmd_stats(int cfd) {
    int connected = 0;
    for (auto &sc : g_shards) if (sc.fd >= 0) connected++;
    std::map<int, std::vector<std::string>> shard_keys;
    for (auto &[k, s] : g_key_shard) shard_keys[s].push_back(k);
    client_send(cfd, "coordinator:     1\n");
    client_send(cfd, "shards total:    " + std::to_string(g_shards.size()) + "\n");
    client_send(cfd, "shards up:       " + std::to_string(connected) + "\n");
    client_send(cfd, "keys tracked:    " + std::to_string(g_key_shard.size()) + "\n");
    client_send(cfd, "puts:            " + std::to_string(g_puts) + "\n");
    client_send(cfd, "gets:            " + std::to_string(g_gets) + "\n");
    client_send(cfd, "deletes:         " + std::to_string(g_dels) + "\n");
    for (auto &sc : g_shards) {
        std::string line = "shard " + std::to_string(sc.id) + ":  "
                         + sc.host + ":" + std::to_string(sc.port)
                         + "  " + (sc.fd >= 0 ? "UP" : "DOWN");
        auto &keys = shard_keys[sc.id];
        if (!keys.empty()) {
            line += "  keys=[";
            for (size_t i = 0; i < keys.size(); i++) { if (i) line += ","; line += keys[i]; }
            line += "]";
        }
        line += "\n";
        client_send(cfd, line);
    }
}

// =============================================================================
// Phase 2: Transaction command handlers (Two-Phase Commit)
// =============================================================================

// ── STAGE a PUT inside a transaction ─────────────────────────────────────────

static void txn_stage_put(int cfd, uint32_t tx_id,
                           const std::string &key, const std::string &val)
{
    auto &txn = g_txns[tx_id];
    int shard_id = g_ring.lookup(key);
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) {
        client_send(cfd, "ERR shard " + std::to_string(shard_id) + " unavailable\n");
        return;
    }

    // Build STAGE body: [op_type=1][key][val]
    std::vector<uint8_t> body;
    body.push_back(1);  // PUT
    encode_str(body, key);
    encode_str(body, val);

    MsgHeader rh{}; std::vector<uint8_t> rb;
    try { roundtrip(sc->fd, STAGE, tx_id, body, rh, rb); }
    catch (const std::exception &e) {
        client_send(cfd, "ERR staging failed: " + std::string(e.what()) + "\n");
        return;
    }

    std::string err;
    if (!parse_simple_resp(rb, err)) {
        client_send(cfd, "ERR staging failed: " + err + "\n");
        return;
    }

    txn.ops.push_back({1, key, val, shard_id});
    txn.shards.insert(shard_id);
    client_send(cfd, "OK staged PUT " + key + " on shard " + std::to_string(shard_id) + "\n");
}

// ── STAGE a DELETE inside a transaction ──────────────────────────────────────

static void txn_stage_del(int cfd, uint32_t tx_id, const std::string &key) {
    auto &txn = g_txns[tx_id];
    int shard_id = g_ring.lookup(key);
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) {
        client_send(cfd, "ERR shard " + std::to_string(shard_id) + " unavailable\n");
        return;
    }

    std::vector<uint8_t> body;
    body.push_back(2);  // DEL
    encode_str(body, key);

    MsgHeader rh{}; std::vector<uint8_t> rb;
    try { roundtrip(sc->fd, STAGE, tx_id, body, rh, rb); }
    catch (const std::exception &e) {
        client_send(cfd, "ERR staging failed: " + std::string(e.what()) + "\n");
        return;
    }

    std::string err;
    if (!parse_simple_resp(rb, err)) {
        client_send(cfd, "ERR staging failed: " + err + "\n");
        return;
    }

    txn.ops.push_back({2, key, "", shard_id});
    txn.shards.insert(shard_id);
    client_send(cfd, "OK staged DEL " + key + " on shard " + std::to_string(shard_id) + "\n");
}

// ── COMMIT: run the full 2PC protocol ────────────────────────────────────────

static void txn_commit(int cfd, uint32_t tx_id) {
    auto it = g_txns.find(tx_id);
    if (it == g_txns.end()) {
        client_send(cfd, "ERR no active transaction\n"); return;
    }
    TxnState &txn = it->second;

    if (txn.ops.empty()) {
        g_txns.erase(it);
        client_send(cfd, "OK (tx " + std::to_string(tx_id) + " committed, empty)\n");
        return;
    }

    std::vector<uint8_t> empty_body;

    // ── Single-shard optimization: skip PREPARE ──────────────────────────
    if (txn.shards.size() == 1) {
        int sid = *txn.shards.begin();
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) {
            client_send(cfd, "ERR shard down during commit\n");
            // abort on all shards to clean staging
            g_txns.erase(it); return;
        }
        send_msg(sc->fd, COMMIT_TXN, tx_id, empty_body);
        MsgHeader r{}; std::vector<uint8_t> rb;
        recv_msg(sc->fd, r, rb);
        // Update key tracking
        for (auto &op : txn.ops) {
            if (op.op_type == 1) g_key_shard[op.key] = op.shard_id;
            else g_key_shard.erase(op.key);
        }
        g_txns.erase(it);
        client_send(cfd, "OK (tx " + std::to_string(tx_id) + " committed, single-shard)\n");
        return;
    }

    // ── Full 2PC: Phase 1 — send PREPARE to all involved shards ──────────
    printf("[coord] 2PC tx %u: sending PREPARE to %zu shards\n",
           tx_id, txn.shards.size());

    bool all_yes = true;
    for (int sid : txn.shards) {
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) { all_yes = false; continue; }
        send_msg(sc->fd, PREPARE, tx_id, empty_body);
    }
    // Collect votes
    for (int sid : txn.shards) {
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) { all_yes = false; continue; }
        MsgHeader r{}; std::vector<uint8_t> rb;
        if (!recv_msg(sc->fd, r, rb) || rb.empty() || rb[0] != 1) {
            all_yes = false;
        }
    }

    printf("[coord] 2PC tx %u: votes collected, decision = %s\n",
           tx_id, all_yes ? "COMMIT" : "ABORT");

    // ── Phase 2: Write decision to coordinator WAL BEFORE sending ────────
    // This is the critical safety guarantee of 2PC.
    if (g_coord_wal.is_open()) {
        if (all_yes) g_coord_wal.append_coord_commit_decision(tx_id, txn.shards);
        else         g_coord_wal.append_coord_abort_decision(tx_id, txn.shards);
    }

    // ── Phase 3: Send decision to all shards ─────────────────────────────
    MsgType decision = all_yes ? COMMIT_TXN : ABORT_TXN;
    for (int sid : txn.shards) {
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) continue;
        send_msg(sc->fd, decision, tx_id, empty_body);
    }
    // Collect ACKs
    for (int sid : txn.shards) {
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) continue;
        MsgHeader r{}; std::vector<uint8_t> rb;
        recv_msg(sc->fd, r, rb);
    }

    if (g_coord_wal.is_open()) g_coord_wal.append_coord_done(tx_id);

    // Update key tracking on commit
    if (all_yes) {
        for (auto &op : txn.ops) {
            if (op.op_type == 1) g_key_shard[op.key] = op.shard_id;
            else g_key_shard.erase(op.key);
        }
    }

    g_txns.erase(it);

    if (all_yes)
        client_send(cfd, "OK (tx " + std::to_string(tx_id) + " committed)\n");
    else
        client_send(cfd, "ABORTED (tx " + std::to_string(tx_id) + ")\n");
}

// ── ABORT: discard all staged ops ────────────────────────────────────────────

static void txn_abort(int cfd, uint32_t tx_id) {
    auto it = g_txns.find(tx_id);
    if (it == g_txns.end()) {
        client_send(cfd, "ERR no active transaction\n"); return;
    }
    TxnState &txn = it->second;

    if (g_coord_wal.is_open()) g_coord_wal.append_coord_abort_decision(tx_id, txn.shards);

    std::vector<uint8_t> empty_body;
    for (int sid : txn.shards) {
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) continue;
        send_msg(sc->fd, ABORT_TXN, tx_id, empty_body);
    }
    for (int sid : txn.shards) {
        ShardConn *sc = find_shard(sid);
        if (!sc || sc->fd < 0) continue;
        MsgHeader r{}; std::vector<uint8_t> rb;
        recv_msg(sc->fd, r, rb);
    }

    if (g_coord_wal.is_open()) g_coord_wal.append_coord_done(tx_id);

    g_txns.erase(it);
    client_send(cfd, "OK (tx " + std::to_string(tx_id) + " aborted)\n");
}

// =============================================================================
// Client connection loop (Phase 1 + Phase 2 commands)
// =============================================================================

static void handle_client(int client_fd) {
    std::string line_buf;
    char ch;
    uint32_t active_tx_id = 0;  // 0 = no active transaction

    auto read_line = [&]() -> bool {
        line_buf.clear();
        while (true) {
            ssize_t r = read(client_fd, &ch, 1);
            if (r <= 0) return false;
            if (ch == '\n') return true;
            if (ch != '\r') line_buf += ch;
        }
    };

    while (read_line()) {
        if (line_buf.empty()) continue;

        std::istringstream iss(line_buf);
        std::string cmd;
        iss >> cmd;
        for (char &c : cmd) c = static_cast<char>(toupper(c));

        if (cmd == "PUT") {
            std::string key, val;
            if (!(iss >> key >> val)) {
                client_send(client_fd, "ERR usage: PUT <key> <value>\n"); continue;
            }
            if (active_tx_id != 0)
                txn_stage_put(client_fd, active_tx_id, key, val);
            else
                cmd_put(client_fd, key, val);

        } else if (cmd == "GET") {
            std::string key;
            if (!(iss >> key)) {
                client_send(client_fd, "ERR usage: GET <key>\n"); continue;
            }
            cmd_get(client_fd, key);

        } else if (cmd == "DELETE" || cmd == "DEL") {
            std::string key;
            if (!(iss >> key)) {
                client_send(client_fd, "ERR usage: DELETE <key>\n"); continue;
            }
            if (active_tx_id != 0)
                txn_stage_del(client_fd, active_tx_id, key);
            else
                cmd_delete(client_fd, key);

        } else if (cmd == "BEGIN") {
            if (active_tx_id != 0) {
                client_send(client_fd, "ERR already in a transaction\n");
            } else {
                active_tx_id = g_next_tx_id++;
                TxnState txn; txn.client_fd = client_fd;
                g_txns[active_tx_id] = txn;
                if (g_coord_wal.is_open()) g_coord_wal.append_coord_begin(active_tx_id);
                printf("[coord] BEGIN tx %u\n", active_tx_id);
                client_send(client_fd,
                    "OK txn started (tx_id=" + std::to_string(active_tx_id) + ")\n");
            }

        } else if (cmd == "COMMIT") {
            if (active_tx_id == 0) {
                client_send(client_fd, "ERR no active transaction\n");
            } else {
                txn_commit(client_fd, active_tx_id);
                active_tx_id = 0;
            }

        } else if (cmd == "ABORT") {
            if (active_tx_id == 0) {
                client_send(client_fd, "ERR no active transaction\n");
            } else {
                txn_abort(client_fd, active_tx_id);
                active_tx_id = 0;
            }

        } else if (cmd == "\\KEYS" || cmd == "\\K" || line_buf == "\\keys") {
            cmd_keys(client_fd);

        } else if (cmd == "\\STATS" || line_buf == "\\stats") {
            cmd_stats(client_fd);

        } else if (cmd == "QUIT" || cmd == "EXIT") {
            // If inside a txn, auto-abort
            if (active_tx_id != 0) {
                txn_abort(client_fd, active_tx_id);
                active_tx_id = 0;
            }
            client_send(client_fd, "BYE\n");
            break;

        } else {
            client_send(client_fd,
                "ERR unknown command '" + cmd + "'\n"
                "    commands: PUT GET DELETE BEGIN COMMIT ABORT \\keys \\stats QUIT\n");
        }
    }

    // Clean up if client disconnected mid-transaction
    if (active_tx_id != 0) {
        printf("[coord] client disconnected mid-tx %u, aborting\n", active_tx_id);
        txn_abort(client_fd, active_tx_id);
    }
}

// =============================================================================
// Startup
// =============================================================================

static void connect_to_shards() {
    for (auto &sc : g_shards) {
        sc.fd = tcp_connect(sc.host, sc.port);
        if (sc.fd >= 0)
            printf("[coord] connected to shard %d (%s:%d)\n", sc.id, sc.host.c_str(), sc.port);
        else
            printf("[coord] WARNING: could not connect to shard %d (%s:%d)\n",
                   sc.id, sc.host.c_str(), sc.port);
        g_shard_map[sc.id] = &sc;
    }
}

// =============================================================================
// main
// =============================================================================

int main(int argc, char *argv[]) {
    std::string shards_spec;
    std::string data_dir = "./coord_data";

    for (int i = 1; i < argc - 1; i++) {
        std::string arg = argv[i];
        if      (arg == "--port")   g_coord_port = std::atoi(argv[++i]);
        else if (arg == "--shards") shards_spec  = argv[++i];
        else if (arg == "--data")   data_dir     = argv[++i];
    }
    if (g_coord_port <= 0 || shards_spec.empty()) usage(argv[0]);

    // Parse shard list
    try { g_shards = parse_shards(shards_spec); }
    catch (const std::exception &e) {
        fprintf(stderr, "[coord] bad --shards spec: %s\n", e.what()); return 1;
    }

    // Build ring
    std::vector<ShardInfo> shard_infos;
    for (auto &sc : g_shards)
        shard_infos.push_back({sc.id, "shard" + std::to_string(sc.id)});
    g_ring.build(shard_infos);
    printf("[coord] ring built: %zu shards, %d vnodes each (%zu total)\n",
           g_shards.size(), Hashring::DEFAULT_VNODES, g_ring.size());

    // Create coord data dir and open coordinator WAL
    std::string mkdir_cmd = "mkdir -p " + data_dir;
    system(mkdir_cmd.c_str());
    std::string wal_path = data_dir + "/coord.wal";

    std::unordered_map<uint32_t, WAL::CoordDecision> pending_decisions;
    uint32_t max_tx_id = 0;
    try {
        WAL::replay_coord(wal_path, pending_decisions, max_tx_id);
        if (max_tx_id >= g_next_tx_id) g_next_tx_id = max_tx_id + 1;
    } catch (const std::exception &e) {
        fprintf(stderr, "[coord] WAL replay failed: %s\n", e.what());
    }

    try { g_coord_wal.open(wal_path); }
    catch (const std::exception &e) {
        fprintf(stderr, "[coord] WAL open failed: %s\n", e.what());
    }

    // Connect to shards
    connect_to_shards();

    // Recover pending transactions
    for (auto &[tx_id, dec] : pending_decisions) {
        printf("[coord] recovering tx %u: decision=%s, %zu shards\n",
               tx_id, dec.commit ? "COMMIT" : "ABORT", dec.shards.size());
        std::vector<uint8_t> empty_body;
        MsgType decision_msg = dec.commit ? COMMIT_TXN : ABORT_TXN;
        for (int sid : dec.shards) {
            ShardConn *sc = find_shard(sid);
            if (!sc || sc->fd < 0) continue;
            send_msg(sc->fd, decision_msg, tx_id, empty_body);
        }
        for (int sid : dec.shards) {
            ShardConn *sc = find_shard(sid);
            if (!sc || sc->fd < 0) continue;
            MsgHeader r{}; std::vector<uint8_t> rb;
            recv_msg(sc->fd, r, rb);
        }
        if (g_coord_wal.is_open()) g_coord_wal.append_coord_done(tx_id);
    }

    // Create TCP listener for clients
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(g_coord_port));
    if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listen_fd, 16) < 0) { perror("listen"); return 1; }
    printf("[coord] ready on port %d\n", g_coord_port);

    // Accept client connections
    while (true) {
        sockaddr_in peer{}; socklen_t peer_len = sizeof(peer);
        int client_fd = accept(listen_fd, reinterpret_cast<sockaddr *>(&peer), &peer_len);
        if (client_fd < 0) { perror("accept"); continue; }
        char peer_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, peer_ip, sizeof(peer_ip));
        printf("[coord] client connected from %s\n", peer_ip);
        handle_client(client_fd);
        close(client_fd);
        printf("[coord] client disconnected\n");
    }
    close(listen_fd);
    return 0;
}