#pragma once
#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>

namespace kvstore
{

    class BloomFilter
    {
    public:
        explicit BloomFilter(size_t n, double fpr = 0.01) : num_keys_(n)
        {
            size_t m = (size_t)ceil(-double(n) * log(fpr) / (log(2.0) * log(2.0)));
            num_bits_ = (m + 63) & ~63ULL;
            bits_.assign(num_bits_ / 64, 0ULL);
            num_hashes_ = std::max(size_t(1), std::min(size_t(20),
                                                       (size_t)round(double(num_bits_) / n * log(2.0))));
        }

        BloomFilter(size_t nh, size_t nb, const uint8_t *d, size_t)
            : num_hashes_(nh), num_bits_(nb), num_keys_(0)
        {
            bits_.resize(nb / 64, 0ULL);
            std::memcpy(bits_.data(), d, nb / 8);
        }

        void Insert(const std::string &key)
        {
            uint64_t h1 = murmur(key.data(), key.size(), 0xDEADBEEFULL);
            uint64_t h2 = murmur(key.data(), key.size(), 0xCAFEBABEULL);
            for (size_t i = 0; i < num_hashes_; i++)
            {
                uint64_t b = (h1 + i * h2) % num_bits_;
                bits_[b / 64] |= (1ULL << (b % 64));
            }
            num_keys_++;
        }
        bool MightContain(const std::string &key) const
        {
            uint64_t h1 = murmur(key.data(), key.size(), 0xDEADBEEFULL);
            uint64_t h2 = murmur(key.data(), key.size(), 0xCAFEBABEULL);
            for (size_t i = 0; i < num_hashes_; i++)
            {
                uint64_t b = (h1 + i * h2) % num_bits_;
                if (!(bits_[b / 64] & (1ULL << (b % 64))))
                    return false;
            }
            return true;
        }
        std::vector<uint8_t> Serialize() const
        {
            std::vector<uint8_t> out;
            auto pu = [&](uint64_t v)
            {
                for (int i = 0; i < 8; i++)
                    out.push_back((v >> (i * 8)) & 0xFF);
            };
            pu(num_hashes_);
            pu(num_bits_);
            pu(num_keys_);
            const uint8_t *raw = reinterpret_cast<const uint8_t *>(bits_.data());
            out.insert(out.end(), raw, raw + num_bits_ / 8);
            return out;
        }

        static BloomFilter Deserialize(const uint8_t *d, size_t len)
        {
            auto ru = [&](size_t o) -> uint64_t
            {
                uint64_t v = 0;
                for (int i = 0; i < 8; i++)
                    v |= uint64_t(d[o + i]) << (i * 8);
                return v;
            };
            return BloomFilter(ru(0), ru(8), d + 24, len - 24);
        }

        size_t NumHashes() const { return num_hashes_; }
        size_t NumBits() const { return num_bits_; }

    private:
        static uint64_t murmur(const void *key, int len, uint64_t seed)
        {
            const uint64_t m = 0xc4ceb9fe1a85ec53ULL;
            const int r = 47;
            uint64_t h = seed ^ (uint64_t(len) * m);
            const uint64_t *d = (const uint64_t *)key, *e = d + (len / 8);
            while (d != e)
            {
                uint64_t x;
                std::memcpy(&x, d++, 8);
                x *= m;
                x ^= x >> r;
                x *= m;
                h ^= x;
                h *= m;
            }
            const uint8_t *t = (const uint8_t *)d;
            switch (len & 7)
            {
            case 7:
                h ^= uint64_t(t[6]) << 48;
                [[fallthrough]];
            case 6:
                h ^= uint64_t(t[5]) << 40;
                [[fallthrough]];
            case 5:
                h ^= uint64_t(t[4]) << 32;
                [[fallthrough]];
            case 4:
                h ^= uint64_t(t[3]) << 24;
                [[fallthrough]];
            case 3:
                h ^= uint64_t(t[2]) << 16;
                [[fallthrough]];
            case 2:
                h ^= uint64_t(t[1]) << 8;
                [[fallthrough]];
            case 1:
                h ^= uint64_t(t[0]);
                h *= m;
            }
            h ^= h >> r;
            h *= m;
            h ^= h >> r;
            return h;
        }

        size_t num_hashes_, num_bits_, num_keys_;
        std::vector<uint64_t> bits_;
    };

#endif
