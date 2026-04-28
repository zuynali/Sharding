#include "proto.hpp"
#include "ring.hpp"

#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

// =============================================================================
// coord.cpp  —  Coordinator process
// =============================================================================
//
// Usage:
//   ./shardkv-coord --port <port> --shards <spec>
//
//   <spec> is a comma-separated list of:  id@host:port
//   Example:
//     ./shardkv-coord --port 6001 \
//                     --shards 1@localhost:7001,2@localhost:7002,3@localhost:7003
//
// Responsibilities (Phase 1):
//   1. Parse the shard list and build the consistent hash ring.
//   2. Open a persistent TCP connection to every shard at startup.
//   3. Accept client connections (plain text, line-oriented).
//   4. For every client command, route to the correct shard and relay
//      the response back to the client.
//   5. Track key→shard assignments for the \keys command.
//
// Client protocol (plain text, one command per line):
//   PUT <key> <value>
//   GET <key>
//   DELETE <key>
//   \keys
//   \stats
//   QUIT
// =============================================================================

// =============================================================================
// Shard connection descriptor
// =============================================================================

struct ShardConn {
    int         id;
    std::string host;
    int         port;
    int         fd = -1;   // -1 means disconnected
};

// =============================================================================
// Global coordinator state
// =============================================================================

static Hashring                           g_ring;
static std::vector<ShardConn>            g_shards;
static std::map<int, ShardConn *>        g_shard_map;   // id → conn
static std::map<std::string, int>        g_key_shard;   // key → shard_id (for \keys)
static int                               g_coord_port = 0;

// Counters for \stats
static long g_puts   = 0;
static long g_gets   = 0;
static long g_dels   = 0;

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

// Parse "1@localhost:7001,2@localhost:7002,3@localhost:7003"
static std::vector<ShardConn> parse_shards(const std::string &spec) {
    std::vector<ShardConn> result;
    std::istringstream ss(spec);
    std::string token;
    while (std::getline(ss, token, ',')) {
        // token = "id@host:port"
        size_t at   = token.find('@');
        size_t col  = token.rfind(':');
        if (at == std::string::npos || col == std::string::npos || col < at)
            throw std::invalid_argument("bad shard spec: " + token);

        ShardConn sc;
        sc.id   = std::stoi(token.substr(0, at));
        sc.host = token.substr(at + 1, col - at - 1);
        sc.port = std::stoi(token.substr(col + 1));
        result.push_back(sc);
    }
    if (result.empty())
        throw std::invalid_argument("empty shard list");
    return result;
}

// =============================================================================
// TCP helpers
// =============================================================================

// Connect to host:port, return fd or -1 on failure.
static int tcp_connect(const std::string &host, int port) {
    addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return -1;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return -1; }

    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        close(fd); freeaddrinfo(res); return -1;
    }
    freeaddrinfo(res);
    return fd;
}

// Send a null-terminated string to a client fd.
static void client_send(int fd, const std::string &s) {
    write_exact(fd, s.data(), s.size());
}

// =============================================================================
// Shard operations  (coordinator → shard binary protocol)
// =============================================================================

static ShardConn *find_shard(int id) {
    auto it = g_shard_map.find(id);
    if (it == g_shard_map.end()) return nullptr;
    return it->second;
}

// Forward a PUT_ONE to the owning shard. Returns true on success.
static bool shard_put(int shard_id, const std::string &key,
                       const std::string &val, std::string &err_out)
{
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) { err_out = "shard unavailable"; return false; }

    MsgHeader rh{};
    std::vector<uint8_t> rb;
    try {
        roundtrip(sc->fd, PUT_ONE, 0, build_put_one(key, val), rh, rb);
    } catch (const std::exception &e) {
        err_out = e.what(); sc->fd = -1; return false;
    }
    return parse_simple_resp(rb, err_out);
}

// Forward a GET_ONE to the owning shard.
static bool shard_get(int shard_id, const std::string &key,
                       std::string &val_out, std::string &err_out)
{
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) { err_out = "shard unavailable"; return false; }

    MsgHeader rh{};
    std::vector<uint8_t> rb;
    try {
        roundtrip(sc->fd, GET_ONE, 0, build_get_one(key), rh, rb);
    } catch (const std::exception &e) {
        err_out = e.what(); sc->fd = -1; return false;
    }
    if (!parse_get_resp(rb, val_out)) { err_out = "not found"; return false; }
    return true;
}

// Forward a DEL_ONE to the owning shard.
static bool shard_del(int shard_id, const std::string &key,
                       std::string &err_out)
{
    ShardConn *sc = find_shard(shard_id);
    if (!sc || sc->fd < 0) { err_out = "shard unavailable"; return false; }

    MsgHeader rh{};
    std::vector<uint8_t> rb;
    try {
        roundtrip(sc->fd, DEL_ONE, 0, build_del_one(key), rh, rb);
    } catch (const std::exception &e) {
        err_out = e.what(); sc->fd = -1; return false;
    }
    return parse_simple_resp(rb, err_out);
}

// =============================================================================
// Client command handlers
// =============================================================================

static void cmd_put(int client_fd, const std::string &key,
                    const std::string &val)
{
    int sid = g_ring.lookup(key);
    std::string err;
    if (shard_put(sid, key, val, err)) {
        g_key_shard[key] = sid;
        g_puts++;
        client_send(client_fd, "OK (shard " + std::to_string(sid) + ")\n");
    } else {
        client_send(client_fd, "ERR " + err + "\n");
    }
}

static void cmd_get(int client_fd, const std::string &key) {
    int sid = g_ring.lookup(key);
    std::string val, err;
    if (shard_get(sid, key, val, err)) {
        g_gets++;
        client_send(client_fd, val + " (shard " + std::to_string(sid) + ")\n");
    } else {
        client_send(client_fd, "ERR " + err + "\n");
    }
}

static void cmd_delete(int client_fd, const std::string &key) {
    int sid = g_ring.lookup(key);
    std::string err;
    if (shard_del(sid, key, err)) {
        g_key_shard.erase(key);
        g_dels++;
        client_send(client_fd, "OK (shard " + std::to_string(sid) + ")\n");
    } else {
        client_send(client_fd, "ERR " + err + "\n");
    }
}

static void cmd_keys(int client_fd) {
    if (g_key_shard.empty()) {
        client_send(client_fd, "(no keys)\n");
        return;
    }
    for (auto &[key, sid] : g_key_shard)
        client_send(client_fd,
                    key + " -> shard " + std::to_string(sid) + "\n");
}

static void cmd_stats(int client_fd) {
    // Count connected shards and total known keys per shard
    int connected = 0;
    for (auto &sc : g_shards)
        if (sc.fd >= 0) connected++;

    std::map<int, std::vector<std::string>> shard_keys;
    for (auto &[k, s] : g_key_shard) shard_keys[s].push_back(k);

    client_send(client_fd, "coordinator:     1\n");
    client_send(client_fd,
                "shards total:    " + std::to_string(g_shards.size()) + "\n");
    client_send(client_fd,
                "shards up:       " + std::to_string(connected) + "\n");
    client_send(client_fd,
                "keys tracked:    " + std::to_string(g_key_shard.size()) + "\n");
    client_send(client_fd, "puts:            " + std::to_string(g_puts) + "\n");
    client_send(client_fd, "gets:            " + std::to_string(g_gets) + "\n");
    client_send(client_fd, "deletes:         " + std::to_string(g_dels) + "\n");

    for (auto &sc : g_shards) {
        std::string line = "shard " + std::to_string(sc.id) + ":  "
                         + sc.host + ":" + std::to_string(sc.port)
                         + "  " + (sc.fd >= 0 ? "UP" : "DOWN");
        auto &keys = shard_keys[sc.id];
        if (!keys.empty()) {
            line += "  keys=[";
            for (size_t i = 0; i < keys.size(); i++) {
                if (i) line += ",";
                line += keys[i];
            }
            line += "]";
        }
        line += "\n";
        client_send(client_fd, line);
    }
}

// =============================================================================
// Client connection loop
// =============================================================================
// Reads one line at a time from the client (plain text).
// Analogy: the coordinator is a phone switchboard operator — it reads what
// the caller says, works out which shard to connect them to, relays the
// message, and reads the response back to the caller.
// =============================================================================

static void handle_client(int client_fd) {
    // Read lines manually — avoids FILE* buffering issues with fgets on sockets
    std::string line_buf;
    char ch;

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

        // Normalise to uppercase for convenience
        for (char &c : cmd) c = static_cast<char>(toupper(c));

        if (cmd == "PUT") {
            std::string key, val;
            if (!(iss >> key >> val)) {
                client_send(client_fd, "ERR usage: PUT <key> <value>\n");
                continue;
            }
            cmd_put(client_fd, key, val);

        } else if (cmd == "GET") {
            std::string key;
            if (!(iss >> key)) {
                client_send(client_fd, "ERR usage: GET <key>\n");
                continue;
            }
            cmd_get(client_fd, key);

        } else if (cmd == "DELETE" || cmd == "DEL") {
            std::string key;
            if (!(iss >> key)) {
                client_send(client_fd, "ERR usage: DELETE <key>\n");
                continue;
            }
            cmd_delete(client_fd, key);

        } else if (cmd == "\\KEYS" || cmd == "\\K" || line_buf == "\\keys") {
            cmd_keys(client_fd);

        } else if (cmd == "\\STATS" || line_buf == "\\stats") {
            cmd_stats(client_fd);

        } else if (cmd == "QUIT" || cmd == "EXIT") {
            client_send(client_fd, "BYE\n");
            break;

        } else {
            client_send(client_fd,
                        "ERR unknown command '" + cmd + "'\n"
                        "    commands: PUT GET DELETE \\keys \\stats QUIT\n");
        }
    }
}

// =============================================================================
// Startup: connect to all shards
// =============================================================================

static void connect_to_shards() {
    for (auto &sc : g_shards) {
        sc.fd = tcp_connect(sc.host, sc.port);
        if (sc.fd >= 0) {
            printf("[coord] connected to shard %d (%s:%d)\n",
                   sc.id, sc.host.c_str(), sc.port);
        } else {
            printf("[coord] WARNING: could not connect to shard %d (%s:%d)"
                   " — it will show as DOWN\n",
                   sc.id, sc.host.c_str(), sc.port);
        }
        g_shard_map[sc.id] = &sc;
    }
}

// =============================================================================
// main
// =============================================================================

int main(int argc, char *argv[]) {
    // ── Parse args ────────────────────────────────────────────────────────
    std::string shards_spec;
    for (int i = 1; i < argc - 1; i++) {
        std::string arg = argv[i];
        if      (arg == "--port")   g_coord_port = std::atoi(argv[++i]);
        else if (arg == "--shards") shards_spec  = argv[++i];
    }
    if (g_coord_port <= 0 || shards_spec.empty()) usage(argv[0]);

    // ── Parse shard list ──────────────────────────────────────────────────
    try {
        g_shards = parse_shards(shards_spec);
    } catch (const std::exception &e) {
        fprintf(stderr, "[coord] bad --shards spec: %s\n", e.what());
        return 1;
    }

    // ── Build ring ────────────────────────────────────────────────────────
    std::vector<ShardInfo> shard_infos;
    for (auto &sc : g_shards)
        shard_infos.push_back({sc.id, "shard" + std::to_string(sc.id)});

    g_ring.build(shard_infos);
    printf("[coord] ring built: %zu shards, %d virtual nodes each (%zu total)\n",
           g_shards.size(),
           Hashring::DEFAULT_VNODES,
           g_ring.size());

    // ── Connect to shards ─────────────────────────────────────────────────
    connect_to_shards();

    // ── Create TCP listener for clients ───────────────────────────────────
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

    // ── Accept client connections ─────────────────────────────────────────
    // Phase 1: single-threaded, one client at a time.
    while (true) {
        sockaddr_in peer{};
        socklen_t   peer_len = sizeof(peer);
        int client_fd = accept(listen_fd,
                               reinterpret_cast<sockaddr *>(&peer), &peer_len);
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