#include "proto.hpp"

#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

// =============================================================================
// cli.cpp  —  Client REPL for the sharded KV store
// =============================================================================
//
// Usage:
//   ./shardkv-cli <host> <port>
//
// Example:
//   ./shardkv-cli localhost 6001
//
// Sends plain-text commands to the coordinator and prints responses.
// One command per line.  The coordinator closes the connection on QUIT.
//
// Supported commands (coordinator handles them, not the CLI):
//   PUT <key> <value>
//   GET <key>
//   DELETE <key>
//   \keys
//   \stats
//   QUIT
// =============================================================================

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s <host> <port>\n", prog);
    fprintf(stderr, "Example: %s localhost 6001\n", prog);
    exit(1);
}

// Connect to host:port, return fd or exit on failure.
static int tcp_connect(const char *host, int port) {
    addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    int rc = getaddrinfo(host, port_str.c_str(), &hints, &res);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
        exit(1);
    }

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { perror("socket"); exit(1); }

    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        perror("connect"); exit(1);
    }
    freeaddrinfo(res);
    return fd;
}

// Read from fd until '\n', return the line (without '\n').
// Returns false on EOF / connection closed.
static bool read_line(int fd, std::string &out) {
    out.clear();
    char ch;
    while (true) {
        ssize_t r = read(fd, &ch, 1);
        if (r <= 0) return false;
        if (ch == '\n') return true;
        if (ch != '\r') out += ch;
    }
}

// Read all response lines until the server stops sending.
// The coordinator sends a fixed number of lines per command, but we don't
// know how many — so we read until the coordinator goes quiet for a moment.
//
// Simpler approach: read lines until we get a line that looks like a final
// response. For our protocol, every command sends at least one line, and
// multi-line responses (\\keys, \\stats) end naturally when the coordinator
// stops writing. We use MSG_PEEK + non-blocking to detect this, but the
// simplest correct approach for a REPL is: read until a prompt-worthy line.
//
// Since our protocol is synchronous (one request → N response lines, then
// the coordinator waits for the next command), we can use a short select()
// timeout to detect end of response.
static void print_response(int fd) {
    fd_set rfds;
    timeval tv;
    std::string line;

    while (true) {
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        tv.tv_sec  = 0;
        tv.tv_usec = 50000;   // 50ms — coordinator always responds faster

        int ready = select(fd + 1, &rfds, nullptr, nullptr, &tv);
        if (ready <= 0) break;   // timeout = no more lines coming

        if (!read_line(fd, line)) break;   // connection closed
        printf("%s\n", line.c_str());

        // If we got BYE (response to QUIT), stop reading
        if (line == "BYE") break;
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) usage(argv[0]);

    const char *host = argv[1];
    int         port = std::atoi(argv[2]);
    if (port <= 0) usage(argv[0]);

    int fd = tcp_connect(host, port);
    printf("connected to coordinator at %s:%d\n\n", host, port);

    // REPL loop
    std::string line;
    while (true) {
        // Print prompt only when stdin is a terminal
        if (isatty(STDIN_FILENO))
            printf("> ");

        if (!std::getline(std::cin, line)) break;   // EOF (Ctrl-D)
        if (line.empty()) continue;

        // Send command to coordinator (add newline)
        std::string to_send = line + "\n";
        if (!write_exact(fd, to_send.data(), to_send.size())) {
            fprintf(stderr, "connection lost\n");
            break;
        }

        // Print whatever the coordinator sends back
        print_response(fd);

        // Stop after QUIT
        std::string cmd;
        for (char c : line) cmd += static_cast<char>(toupper(c));
        if (cmd == "QUIT" || cmd == "EXIT") break;
    }

    close(fd);
    return 0;
}