#pragma once

#include <arpa/inet.h>    // htonl, htons, ntohl, ntohs
#include <cstdint>
#include <cstring>        // memcpy
#include <stdexcept>
#include <string>
#include <unistd.h>       // read, write
#include <vector>

enum MsgType : uint16_t {
    // Phase 1.... single-key operations
    PUT_ONE       = 1,
    PUT_ONE_RESP  = 2,
    GET_ONE       = 3,
    GET_ONE_RESP  = 4,
    DEL_ONE       = 5,
    DEL_ONE_RESP  = 6,

    STAGE         = 10,
    STAGE_RESP    = 11,
    PREPARE       = 20,
    PREPARE_RESP  = 21,
    COMMIT_TXN    = 30,
    COMMIT_RESP   = 31,
    ABORT_TXN     = 40,
    ABORT_RESP    = 41,
};

static constexpr uint8_t STATUS_OK    = 1;
static constexpr uint8_t STATUS_ERROR = 0;

struct MsgHeader {
    uint32_t msg_len;    // total length including this 12-byte header
    uint16_t msg_type;
    uint16_t flags;      
    uint32_t tx_id;      
};

static constexpr size_t HEADER_SIZE = 12;  

inline bool read_exact(int fd, void *buf, size_t n) {
    char    *p         = static_cast<char *>(buf);
    size_t   remaining = n;
    while (remaining > 0) {
        ssize_t r = ::read(fd, p, remaining);
        if (r <= 0) return false;   // EOF (0) or error (-1)
        p         += r;
        remaining -= static_cast<size_t>(r);
    }
    return true;
}

inline bool write_exact(int fd, const void *buf, size_t n) {
    const char *p         = static_cast<const char *>(buf);
    size_t      remaining = n;
    while (remaining > 0) {
        ssize_t w = ::write(fd, p, remaining);
        if (w <= 0) return false;
        p         += w;
        remaining -= static_cast<size_t>(w);
    }
    return true;
}


inline void encode_header(uint8_t out[HEADER_SIZE], const MsgHeader &h) {
    uint32_t ml = htonl(h.msg_len);
    uint16_t mt = htons(h.msg_type);
    uint16_t fl = htons(h.flags);
    uint32_t ti = htonl(h.tx_id);
    std::memcpy(out + 0, &ml, 4);
    std::memcpy(out + 4, &mt, 2);
    std::memcpy(out + 6, &fl, 2);
    std::memcpy(out + 8, &ti, 4);
}

inline MsgHeader decode_header(const uint8_t in[HEADER_SIZE]) {
    MsgHeader h{};
    uint32_t ml; std::memcpy(&ml, in + 0, 4); h.msg_len  = ntohl(ml);
    uint16_t mt; std::memcpy(&mt, in + 4, 2); h.msg_type = ntohs(mt);
    uint16_t fl; std::memcpy(&fl, in + 6, 2); h.flags    = ntohs(fl);
    uint32_t ti; std::memcpy(&ti, in + 8, 4); h.tx_id    = ntohl(ti);
    return h;
}

inline bool send_msg(int fd, MsgType type, uint32_t tx_id,
                     const void *body, size_t body_len)
{
    MsgHeader h;
    h.msg_len  = static_cast<uint32_t>(HEADER_SIZE + body_len);
    h.msg_type = static_cast<uint16_t>(type);
    h.flags    = 0;
    h.tx_id    = tx_id;

    uint8_t hdr_buf[HEADER_SIZE];
    encode_header(hdr_buf, h);

    if (!write_exact(fd, hdr_buf, HEADER_SIZE)) return false;
    if (body_len > 0 && !write_exact(fd, body, body_len)) return false;
    return true;
}

inline bool send_msg(int fd, MsgType type, uint32_t tx_id,
                     const std::vector<uint8_t> &body)
{
    return send_msg(fd, type, tx_id,
                    body.empty() ? nullptr : body.data(), body.size());
}

inline bool recv_msg(int fd, MsgHeader &hdr_out, std::vector<uint8_t> &body_out) {
    uint8_t hdr_buf[HEADER_SIZE];
    if (!read_exact(fd, hdr_buf, HEADER_SIZE)) return false;   // EOF / error

    hdr_out = decode_header(hdr_buf);

    if (hdr_out.msg_len < HEADER_SIZE)
        throw std::runtime_error("recv_msg: msg_len smaller than header");

    size_t body_len = hdr_out.msg_len - HEADER_SIZE;
    body_out.resize(body_len);
    if (body_len > 0 && !read_exact(fd, body_out.data(), body_len))
        throw std::runtime_error("recv_msg: connection dropped mid-body");

    return true;
}

inline void encode_str(std::vector<uint8_t> &buf, const std::string &s) {
    uint32_t net_len = htonl(static_cast<uint32_t>(s.size()));
    const uint8_t *p = reinterpret_cast<const uint8_t *>(&net_len);
    buf.insert(buf.end(), p, p + 4);
    buf.insert(buf.end(), s.begin(), s.end());
}

inline std::string decode_str(const std::vector<uint8_t> &body, size_t &offset) {
    if (offset + 4 > body.size())
        throw std::runtime_error("decode_str: buffer too small for length prefix");
    uint32_t net_len;
    std::memcpy(&net_len, body.data() + offset, 4);
    uint32_t len = ntohl(net_len);
    offset += 4;
    if (offset + len > body.size())
        throw std::runtime_error("decode_str: buffer too small for string body");
    std::string s(reinterpret_cast<const char *>(body.data() + offset), len);
    offset += len;
    return s;
}

inline std::vector<uint8_t> build_put_one(const std::string &key,
                                           const std::string &val)
{
    std::vector<uint8_t> body;
    body.reserve(8 + key.size() + val.size());
    encode_str(body, key);
    encode_str(body, val);
    return body;
}

inline std::vector<uint8_t> build_get_one(const std::string &key) {
    std::vector<uint8_t> body;
    body.reserve(4 + key.size());
    encode_str(body, key);
    return body;
}

inline std::vector<uint8_t> build_del_one(const std::string &key) {
    return build_get_one(key);   // same layout as GET_ONE
}
inline void parse_put_one(const std::vector<uint8_t> &body,
                           std::string &key_out, std::string &val_out)
{
    size_t offset = 0;
    key_out = decode_str(body, offset);
    val_out = decode_str(body, offset);
}
inline void parse_get_one(const std::vector<uint8_t> &body,
                           std::string &key_out)
{
    size_t offset = 0;
    key_out = decode_str(body, offset);
}

inline std::vector<uint8_t> build_resp_ok() {
    return { STATUS_OK };
}

inline std::vector<uint8_t> build_resp_err(const std::string &reason) {
    std::vector<uint8_t> body = { STATUS_ERROR };
    body.insert(body.end(), reason.begin(), reason.end());
    return body;
}

inline std::vector<uint8_t> build_get_resp(bool found, const std::string &val) {
    if (!found) return { STATUS_ERROR };
    std::vector<uint8_t> body = { STATUS_OK };
    encode_str(body, val);
    return body;
}
inline bool parse_simple_resp(const std::vector<uint8_t> &body,
                               std::string &err_out)
{
    if (body.empty()) { err_out = "empty response"; return false; }
    if (body[0] == STATUS_OK) return true;
    err_out = std::string(reinterpret_cast<const char *>(body.data() + 1),
                          body.size() - 1);
    return false;
}
inline bool parse_get_resp(const std::vector<uint8_t> &body,
                            std::string &val_out)
{
    if (body.empty() || body[0] != STATUS_OK) return false;
    size_t offset = 1;
    val_out = decode_str(body, offset);
    return true;
}
inline bool roundtrip(int fd,
                      MsgType req_type, uint32_t tx_id,
                      const std::vector<uint8_t> &req_body,
                      MsgHeader &resp_hdr_out,
                      std::vector<uint8_t> &resp_body_out)
{
    if (!send_msg(fd, req_type, tx_id, req_body))
        throw std::runtime_error("roundtrip: send failed");
    if (!recv_msg(fd, resp_hdr_out, resp_body_out))
        throw std::runtime_error("roundtrip: connection closed before response");
    return true;
}