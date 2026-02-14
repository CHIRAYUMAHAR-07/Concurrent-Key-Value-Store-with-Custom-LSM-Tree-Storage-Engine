#pragma once
#ifndef WAL_H
#define WAL_H

#include <string>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <functional>
#include <vector>
#include <stdexcept>
#include <cstring>

namespace kvstore
{

    enum class WALType : uint8_t
    {
        PUT = 0x01,
        DEL = 0x02,
        CHECKPOINT = 0x03
    };
    static constexpr uint32_t WAL_MAGIC = 0xDEADF00D;

    struct WALRecord
    {
        WALType type;
        uint64_t seq;
        std::string key, value;
    };

    inline uint32_t crc32c(const uint8_t *d, size_t n)
    {
        uint32_t c = 0xFFFFFFFF;
        for (size_t i = 0; i < n; i++)
        {
            c ^= d[i];
            for (int j = 0; j < 8; j++)
                c = (c >> 1) ^ (0xEDB88320u & -(c & 1));
        }
        return c ^ 0xFFFFFFFF;
    }

    class WAL
    {
    public:
        explicit WAL(const std::string &path) : path_(path)
        {
            f_.open(path, std::ios::binary | std::ios::app | std::ios::out);
            if (!f_)
                throw std::runtime_error("WAL open failed: " + path);
        }

        ~WAL()
        {
            if (f_)
            {
                f_.flush();
                f_.close();
            }
        }

        void AppendPut(uint64_t seq, const std::string &k, const std::string &v)
        {
            append(WALType::PUT, seq, k, v);
        }

        void AppendDel(uint64_t seq, const std::string &k)
        {
            append(WALType::DEL, seq, k, "");
        }

        void Checkpoint(uint64_t seq)
        {
            append(WALType::CHECKPOINT, seq, "", "");
        }

        void Sync()
        {
            std::lock_guard lk(mu_);
            f_.flush();
        }

        static size_t Replay(const std::string &path, uint64_t last_ckpt_seq,
                             std::function<void(const WALRecord &)> cb)
        {
            std::ifstream in(path, std::ios::binary);
            if (!in)
                return 0;

            size_t count = 0;

            while (in.good())
            {
                WALRecord r;
                if (!readRecord(in, r))
                    break;
                if (r.type == WALType::CHECKPOINT)
                    continue;
                if (r.seq > last_ckpt_seq)
                {
                    cb(r);
                    count++;
                }
            }
            return count;
        }

        void Rotate(const std::string &new_path)
        {
            std::lock_guard lk(mu_);
            f_.flush();
            f_.close();
            std::rename(path_.c_str(), (path_ + ".old").c_str());
            path_ = new_path;
            f_.open(new_path, std::ios::binary | std::ios::app);
        }

    private:
        void append(WALType t, uint64_t seq, const std::string &k, const std::string &v)
        {
            std::vector<uint8_t> buf;
            buf.reserve(25 + k.size() + v.size());

            auto pu32 = [&](uint32_t x)
            {
                for (int i = 0; i < 4; i++)
                    buf.push_back((x >> (i * 8)) & 0xFF);
            };

            auto pu64 = [&](uint64_t x)
            {
                for (int i = 0; i < 8; i++)
                    buf.push_back((x >> (i * 8)) & 0xFF);
            };

            auto pstr = [&](const std::string &s)
            {
                buf.insert(buf.end(), s.begin(), s.end());
            };

            pu32(WAL_MAGIC);
            buf.push_back(uint8_t(t));
            pu64(seq);
            pu32(uint32_t(k.size()));
            pstr(k);
            pu32(uint32_t(v.size()));
            pstr(v);
            pu32(crc32c(buf.data(), buf.size()));

            std::lock_guard lk(mu_);
            f_.write(reinterpret_cast<const char *>(buf.data()), buf.size());
            f_.flush();
        }

        static bool readRecord(std::ifstream &in, WALRecord &r)
        {
            auto r32 = [&](uint32_t &v) -> bool
            {
                uint8_t b[4];
                if (!in.read((char *)b, 4))
                    return false;
                v = b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24);
                return true;
            };

            auto r64 = [&](uint64_t &v) -> bool
            {
                uint8_t b[8];
                if (!in.read((char *)b, 8))
                    return false;
                v = 0;
                for (int i = 0; i < 8; i++)
                    v |= uint64_t(b[i]) << (i * 8);
                return true;
            };

            uint32_t magic = 0;
            if (!r32(magic) || magic != WAL_MAGIC)
                return false;

            uint8_t t = 0;
            if (!in.read((char *)&t, 1))
                return false;
            r.type = WALType(t);

            if (!r64(r.seq))
                return false;

            uint32_t kl = 0, vl = 0;

            if (!r32(kl))
                return false;
            r.key.resize(kl);
            if (kl && !in.read(r.key.data(), kl))
                return false;

            if (!r32(vl))
                return false;
            r.value.resize(vl);
            if (vl && !in.read(r.value.data(), vl))
                return false;

            uint32_t crc = 0;
            r32(crc);

            return true;
        }

        std::string path_;
        std::ofstream f_;
        std::mutex mu_;
    };

}

#endif