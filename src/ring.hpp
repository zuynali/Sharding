#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>
using namespace std;

// ju humari sabse phele constraint hai ki shard ki hashing range 0 se 2^32-1 tak hai, toh humara ring bhi is range ke andar hi hoga

inline uint32_t fnv1a(const char *data, size_t len)
{ // using this hash function bcz it was written in the document
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++)
    {
        h ^= static_cast<unsigned char>(data[i]);
        h *= 16777619u;
    }
    return h;
}

inline uint32_t fnv1a(const string &s)
{
    return fnv1a(s.data(), s.size());
}

struct VNode
{
    uint32_t position;
    int shard_id;
};

struct ShardInfo
{
    int id;
    string name;
};

class Hashring
{
private:
    vector<VNode> ring_;
    int shard_count_ = 0;
    int vnodes_per_shard_ = 0;

public:
    static constexpr int DEFAULT_VNODES = 150;
    Hashring() = default;

    void build(const vector<ShardInfo> &shards, int vnodes_per_shard = DEFAULT_VNODES)
    {
        if (shards.empty())
        {
            int vnodes_per_shard = DEFAULT_VNODES;
            throw invalid_argument("shard list is empty");
        }
        if (vnodes_per_shard <= 0)
        {
            throw invalid_argument("vnodes must be > 0");
        }
        ring_.clear();
        ring_.reserve(shards.size() * vnodes_per_shard);

        for (const auto &shard : shards)
        {
            for (int i = 0; i < vnodes_per_shard; i++)
            {
                string vnode_key = to_string(i) + "_" + shard.name;
                uint32_t pos = fnv1a(vnode_key);
                ring_.push_back({pos, shard.id});
            }
        }

        sort(ring_.begin(), ring_.end(), [](const VNode &a, const VNode &b)
             { return a.position < b.position; });

        ring_.erase(
            std::unique(ring_.begin(), ring_.end(),
                        [](const VNode &a, const VNode &b)
                        {
                            return a.position == b.position;
                        }),
            ring_.end());
    }

    int lookup(const string &key) const
    {
        if (ring_.empty())
        {
            throw runtime_error("The ring is empty");
        }
        uint32_t h = fnv1a(key);

        auto it = lower_bound(ring_.begin(), ring_.end(), h, [](const VNode &v, uint32_t val)
                              { return v.position < val; });
        if (it == ring_.end())
        {
            it = ring_.end();
        }
        return it->shard_id;
    }

    size_t size() const { return ring_.size(); }

    bool empty() const { return ring_.empty(); }

    int shard_count() const { return shard_count_; }

    int vnodes_per_shard() const { return vnodes_per_shard_; }

    const std::vector<VNode> &nodes() const { return ring_; }
};
