// ring_test.cpp
//
// Compile & run:
//   g++ -std=c++17 -O2 -Wall -o ring_test ring_test.cpp && ./ring_test
//
// Tests required by the project spec (Section 4.6):
//   1. Distribution uniformity  – 100,000 keys, each shard gets 28–38%
//   2. Minimal key movement     – adding a 3rd shard moves only ~1/3 of keys
//
// Plus edge-case and sanity tests.

#include "../src/ring.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

// =============================================================================
// Minimal test framework
// =============================================================================

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(expr)                                                            \
    do {                                                                       \
        tests_run++;                                                           \
        if (expr) {                                                            \
            tests_passed++;                                                    \
            printf("  [PASS] %s\n", #expr);                                   \
        } else {                                                               \
            printf("  [FAIL] %s  (line %d)\n", #expr, __LINE__);              \
        }                                                                      \
    } while (0)

#define CHECK_MSG(expr, msg)                                                   \
    do {                                                                       \
        tests_run++;                                                           \
        if (expr) {                                                            \
            tests_passed++;                                                    \
            printf("  [PASS] %s\n", msg);                                     \
        } else {                                                               \
            printf("  [FAIL] %s\n", msg);                                     \
        }                                                                      \
    } while (0)

static void section(const char *name) {
    printf("\n── %s ──\n", name);
}

// =============================================================================
// Helpers
// =============================================================================

// Build a ring with N shards named "shard1", "shard2", ...
static Hashring make_ring(int n, int vnodes = Hashring::DEFAULT_VNODES) {
    std::vector<ShardInfo> shards;
    for (int i = 1; i <= n; i++)
        shards.push_back({i, "shard" + std::to_string(i)});
    Hashring ring;
    ring.build(shards, vnodes);
    return ring;
}

// Generate a deterministic key like "key_000042"
static std::string make_key(int i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%06d", i);
    return buf;
}

// =============================================================================
// Test 1: fnv1a basic correctness
// =============================================================================

static void test_fnv1a() {
    section("fnv1a hash");

    // Known FNV-1a 32-bit values (reference vectors)
    CHECK(fnv1a("") == 2166136261u);          // empty string = offset basis
    CHECK(fnv1a("a") == 3826002220u);
    CHECK(fnv1a("foobar") == 0xbf9cf968u);

    // Determinism: same input always same output
    CHECK(fnv1a("hello") == fnv1a("hello"));
    CHECK(fnv1a("hello") != fnv1a("Hello")); // case sensitive

    // The std::string and const char* overloads agree
    std::string s = "consistent_hashing";
    CHECK(fnv1a(s) == fnv1a(s.data(), s.size()));
}

// =============================================================================
// Test 2: Ring construction sanity
// =============================================================================

static void test_ring_construction() {
    section("Ring construction");

    Hashring ring = make_ring(3, 150);

    // Should have close to 3*150 = 450 vnodes
    // (small loss due to position deduplication is fine)
    CHECK(ring.size() >= 440 && ring.size() <= 450);
    CHECK(ring.shard_count() == 3);
    CHECK(ring.vnodes_per_shard() == 150);

    // Ring must be sorted by position (binary search depends on this)
    const auto &nodes = ring.nodes();
    bool sorted = true;
    for (size_t i = 1; i < nodes.size(); i++)
        if (nodes[i].position < nodes[i - 1].position)
            sorted = false;
    CHECK(sorted);

    // All shard ids in the ring must be 1, 2, or 3
    bool valid_ids = true;
    for (const auto &v : nodes)
        if (v.shard_id < 1 || v.shard_id > 3)
            valid_ids = false;
    CHECK(valid_ids);
}

// =============================================================================
// Test 3: Determinism – same key always maps to same shard
// =============================================================================

static void test_determinism() {
    section("Determinism");

    Hashring r1 = make_ring(3);
    Hashring r2 = make_ring(3);    // rebuilt from scratch

    bool same = true;
    for (int i = 0; i < 1000; i++) {
        std::string k = make_key(i);
        if (r1.lookup(k) != r2.lookup(k)) { same = false; break; }
    }
    CHECK_MSG(same, "Two rings built with same args give identical lookups");

    // A ring built with a different vnode count must give the same lookup for
    // the common case (150 vs 150 here, but different object instances)
    Hashring r3 = make_ring(3, 150);
    bool same2 = true;
    for (int i = 0; i < 1000; i++) {
        std::string k = make_key(i);
        if (r1.lookup(k) != r3.lookup(k)) { same2 = false; break; }
    }
    CHECK_MSG(same2, "Ring rebuilt with same vnode count is identical");
}

// =============================================================================
// Test 4 (Required): Distribution uniformity
//
// Spec says: "With 150 virtual nodes, each shard should receive between
// 28% and 38% of the keys."  We use 100,000 random keys as directed.
// =============================================================================

static void test_distribution() {
    section("Distribution uniformity (100,000 keys, 3 shards)");

    Hashring ring = make_ring(3, 150);

    std::map<int, int> counts;
    const int N = 100000;
    for (int i = 0; i < N; i++) {
        std::string k = "randomkey_" + std::to_string(i) + "_x" +
                        std::to_string(i * 2654435761u);   // spread nicely
        int sid = ring.lookup(k);
        counts[sid]++;
    }

    printf("  Distribution over %d keys:\n", N);
    bool all_in_range = true;
    for (auto &[sid, cnt] : counts) {
        double pct = 100.0 * cnt / N;
        printf("    shard %d: %d keys (%.1f%%)\n", sid, cnt, pct);
        if (pct < 28.0 || pct > 38.0) all_in_range = false;
    }
    CHECK_MSG(all_in_range,
              "Each shard receives 28%–38% of keys (spec requirement)");
}

// =============================================================================
// Test 5 (Required): Minimal key movement when a shard is added
//
// Spec says: "Build a ring with two shards and look up 10,000 keys.
// Then rebuild the ring with three shards and look up the same 10,000 keys.
// Count how many changed owners.  The expected fraction is about 1/3."
//
// We accept anything in [20%, 45%] — the expectation is ~33%.
// A dramatically higher fraction means the ring is not deterministic
// (probably forgot to sort).
// =============================================================================

static void test_minimal_movement() {
    section("Minimal key movement on shard addition (10,000 keys)");

    Hashring ring2 = make_ring(2, 150);
    Hashring ring3 = make_ring(3, 150);

    const int N = 10000;
    int moved = 0;
    for (int i = 0; i < N; i++) {
        std::string k = make_key(i);
        if (ring2.lookup(k) != ring3.lookup(k))
            moved++;
    }

    double pct = 100.0 * moved / N;
    printf("  Keys that changed shard when adding shard3: %d / %d  (%.1f%%)\n",
           moved, N, pct);

    // Expected ~1/3 ≈ 33%.  Allow generous margin for hash variance.
    CHECK_MSG(pct >= 20.0 && pct <= 45.0,
              "~1/3 of keys move (20%–45%) — consistent hashing property");
}

// =============================================================================
// Test 6: Single-shard ring – every key goes to the only shard
// =============================================================================

static void test_single_shard() {
    section("Single-shard ring");

    Hashring ring = make_ring(1, 150);
    bool all_same = true;
    for (int i = 0; i < 500; i++)
        if (ring.lookup(make_key(i)) != 1)
            all_same = false;
    CHECK_MSG(all_same, "All keys go to shard 1 when only one shard exists");
}

// =============================================================================
// Test 7: Wraparound – a key that hashes past the last vnode
//         must land on the first vnode (clockwise wrap)
// =============================================================================

static void test_wraparound() {
    section("Ring wraparound");

    // We can't force a specific key to hash past the last position without
    // knowing the positions, but we CAN check that lookup never throws or
    // returns an invalid id for any of a large set of keys.
    Hashring ring = make_ring(3, 150);
    bool all_valid = true;
    for (int i = 0; i < 10000; i++) {
        int sid = ring.lookup(make_key(i));
        if (sid < 1 || sid > 3) { all_valid = false; break; }
    }
    CHECK_MSG(all_valid, "lookup always returns a valid shard id");

    // Also test keys that produce uint32_t values near UINT32_MAX
    // by using carefully chosen strings (not guaranteed, but helps)
    for (const char *k : {"zzzzzzzzzzzzzz", "~~~~~~~~~~~~~", "\xff\xff\xff"}) {
        int sid = ring.lookup(k);
        CHECK_MSG(sid >= 1 && sid <= 3, "edge-case key resolves to valid shard");
    }
}

// =============================================================================
// Test 8: Error handling
// =============================================================================

static void test_errors() {
    section("Error handling");

    // lookup on an unbuilt ring must throw
    Hashring ring;
    bool threw = false;
    try { ring.lookup("anything"); }
    catch (const std::runtime_error &) { threw = true; }
    CHECK_MSG(threw, "lookup on empty ring throws runtime_error");

    // build with empty shard list must throw
    bool threw2 = false;
    try { ring.build({}); }
    catch (const std::invalid_argument &) { threw2 = true; }
    CHECK_MSG(threw2, "build with empty shard list throws invalid_argument");

    // build with zero vnodes must throw
    bool threw3 = false;
    try { ring.build({{1, "shard1"}}, 0); }
    catch (const std::invalid_argument &) { threw3 = true; }
    CHECK_MSG(threw3, "build with vnodes=0 throws invalid_argument");
}

// =============================================================================
// main
// =============================================================================

int main() {
    printf("=== Hashring unit tests ===\n");

    test_fnv1a();
    test_ring_construction();
    test_determinism();
    test_distribution();
    test_minimal_movement();
    test_single_shard();
    test_wraparound();
    test_errors();

    printf("\n=== Results: %d / %d passed ===\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}