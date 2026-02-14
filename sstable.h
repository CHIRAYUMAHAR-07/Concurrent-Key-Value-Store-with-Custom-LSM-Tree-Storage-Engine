#pragma once
#ifndef SSTABLE_H
#define SSTABLE_H

#include "bloom_filter.h"
#include "memtable.h"
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <algorithm>
#include <cstring>

namespace kvstore
{

    struct SSTMeta
    {
        std::string path, smallest, largest;
        size_t num_entries = 0, file_size = 0;
        int level = 0;
        uint64_t min_seq = 0, max_seq = 0;
    };

    struct IdxEntry
    {
        std::string key;
        uint64_t offset;
    };

    class SSTWriter
    {
    public:
        explicit SSTWriter(const std::string &p) : path_(p)
        {
            f_.open(p, std::ios::binary | std::ios::trunc);
            if (!f_)
                throw std::runtime_error("SSTWriter: " + p);
        }

        SSTMeta Write(const std::vector<KVPair> &sorted)
        {
            if (sorted.empty())
                throw std::runtime_error("SSTWriter: empty");
            BloomFilter bloom(sorted.size(), 0.01);
            std::vector<IdxEntry> idx;
            uint64_t off = 0;
            uint64_t min_s = UINT64_MAX, max_s = 0;

            for (size_t i = 0; i < sorted.size(); i++)
            {
                auto &e = sorted[i];
                bloom.Insert(e.key);
                if (i % 16 == 0)
                    idx.push_back({e.key, off});
                uint32_t kl = e.key.size(), vl = e.value.size();
                w32(kl);
                f_.write(e.key.data(), kl);
                w32(vl);
                f_.write(e.value.data(), vl);
                uint8_t d = e.deleted ? 1 : 0;
                f_.write((char *)&d, 1);
                w64(e.seq);
                off += 4 + kl + 4 + vl + 1 + 8;
                min_s = std::min(min_s, e.seq);
                max_s = std::max(max_s, e.seq);
            }
            uint64_t idx_off = off;

            w32(uint32_t(idx.size()));
            for (auto &ie : idx)
            {
                w32(ie.key.size());
                f_.write(ie.key.data(), ie.key.size());
                w64(ie.offset);
            }

            uint64_t bloom_off = idx_off + 4;
            for (auto &ie : idx)
                bloom_off += 4 + ie.key.size() + 8;

            auto bb = bloom.Serialize();
            w32(uint32_t(bb.size()));
            f_.write(reinterpret_cast<const char *>(bb.data()), bb.size());

            w64(idx_off);
            w64(bloom_off);
            w64(uint64_t(sorted.size()));
            w64(min_s);
            w32(0xCAFEBABEu);

            f_.flush();
            f_.close();

            SSTMeta m;
            m.path = path_;
            m.smallest = sorted.front().key;
            m.largest = sorted.back().key;
            m.num_entries = sorted.size();
            m.min_seq = min_s;
            m.max_seq = max_s;

            std::ifstream sz(path_, std::ios::ate | std::ios::binary);
            m.file_size = sz.tellg();
            return m;
        }

    private:
        void w32(uint32_t v)
        {
            char b[4];
            for (int i = 0; i < 4; i++)
                b[i] = (v >> (i * 8)) & 0xFF;
            f_.write(b, 4);
        }

        void w64(uint64_t v)
        {
            char b[8];
            for (int i = 0; i < 8; i++)
                b[i] = (v >> (i * 8)) & 0xFF;
            f_.write(b, 8);
        }

        std::string path_;
        std::ofstream f_;
    };

    class SSTReader
    {
    public:
        explicit SSTReader(const std::string &p) : path_(p)
        {
            loadFooter();
            loadIndex();
            loadBloom();
        }

        std::optional<KVPair> Get(const std::string &key)
        {
            if (!bloom_->MightContain(key))
            {
                bloom_saves_++;
                return std::nullopt;
            }

            if (key < smallest_ || key > largest_)
                return std::nullopt;

            std::ifstream in(path_, std::ios::binary);
            if (!in)
                return std::nullopt;

            in.seekg(findOffset(key));

            while (in.good())
            {
                uint32_t kl = 0;
                if (!r32(in, kl) || !kl)
                    break;

                std::string k(kl, '\0');
                if (!in.read(k.data(), kl))
                    break;

                uint32_t vl = 0;
                if (!r32(in, vl))
                    break;

                std::string v(vl, '\0');
                if (vl && !in.read(v.data(), vl))
                    break;

                uint8_t del = 0;
                in.read((char *)&del, 1);

                uint64_t seq = 0;
                r64(in, seq);

                if (k == key)
                    return KVPair{k, v, del != 0, seq};
                if (k > key)
                    break;
            }

            return std::nullopt;
        }

        std::vector<KVPair> Scan(const std::string &s, const std::string &e, size_t lim = SIZE_MAX) const
        {
            std::vector<KVPair> out;
            std::ifstream in(path_, std::ios::binary);
            if (!in)
                return out;

            in.seekg(findOffset(s));

            while (in.good() && out.size() < lim)
            {
                uint32_t kl = 0;
                if (!r32(in, kl) || !kl)
                    break;

                std::string k(kl, '\0');
                if (!in.read(k.data(), kl))
                    break;

                uint32_t vl = 0;
                if (!r32(in, vl))
                    break;

                std::string v(vl, '\0');
                if (vl && !in.read(v.data(), vl))
                    break;

                uint8_t del = 0;
                in.read((char *)&del, 1);

                uint64_t seq = 0;
                r64(in, seq);

                if (!s.empty() && k < s)
                    continue;
                if (!e.empty() && k >= e)
                    break;

                if (!del)
                    out.push_back({k, v, false, seq});
            }

            return out;
        }

        bool Overlaps(const std::string &k) const { return k >= smallest_ && k <= largest_; }
        uint64_t BloomSaves() const { return bloom_saves_; }
        uint64_t NumEntries() const { return num_entries_; }
        const std::string &Smallest() const { return smallest_; }
        const std::string &Largest() const { return largest_; }

    private:
        void loadFooter()
        {
            std::ifstream in(path_, std::ios::binary | std::ios::ate);
            if (!in)
                throw std::runtime_error("SSTReader: " + path_);
            file_size_ = in.tellg();
            in.seekg(file_size_ - 36);
            r64(in, idx_off_);
            r64(in, bloom_off_);
            r64(in, num_entries_);
            uint64_t ms = 0;
            r64(in, ms);
            uint32_t magic = 0;
            r32(in, magic);
        }

        void loadIndex()
        {
            std::ifstream in(path_, std::ios::binary);
            if (!in)
                return;

            in.seekg(idx_off_);

            uint32_t cnt = 0;
            r32(in, cnt);
            idx_.reserve(cnt);

            for (uint32_t i = 0; i < cnt; i++)
            {
                uint32_t kl = 0;
                r32(in, kl);
                std::string k(kl, '\0');
                in.read(k.data(), kl);
                uint64_t o = 0;
                r64(in, o);
                idx_.push_back({k, o});
            }

            if (!idx_.empty())
            {
                smallest_ = idx_.front().key;
                largest_ = idx_.back().key;
            }
        }

        void loadBloom()
        {
            std::ifstream in(path_, std::ios::binary);
            if (!in)
                return;

            in.seekg(bloom_off_);

            uint32_t bsz = 0;
            r32(in, bsz);
            if (!bsz)
                return;

            std::vector<uint8_t> bd(bsz);
            in.read(reinterpret_cast<char *>(bd.data()), bsz);

            bloom_ = std::make_unique<BloomFilter>(
                BloomFilter::Deserialize(bd.data(), bsz));
        }

        uint64_t findOffset(const std::string &key) const
        {
            if (idx_.empty())
                return 0;

            size_t lo = 0, hi = idx_.size();
            while (lo + 1 < hi)
            {
                size_t m = lo + (hi - lo) / 2;
                if (idx_[m].key <= key)
                    lo = m;
                else
                    hi = m;
            }

            return idx_[lo].offset;
        }

        static bool r32(std::ifstream &in, uint32_t &v)
        {
            uint8_t b[4];
            if (!in.read((char *)b, 4))
                return false;
            v = b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24);
            return true;
        }

        static bool r64(std::ifstream &in, uint64_t &v)
        {
            uint8_t b[8];
            if (!in.read((char *)b, 8))
                return false;
            v = 0;
            for (int i = 0; i < 8; i++)
                v |= uint64_t(b[i]) << (i * 8);
            return true;
        }

        std::string path_;
        uint64_t file_size_ = 0, idx_off_ = 0, bloom_off_ = 0, num_entries_ = 0;
        std::vector<IdxEntry> idx_;
        std::unique_ptr<BloomFilter> bloom_;
        std::string smallest_, largest_;
        mutable uint64_t bloom_saves_ = 0;
    };

}

#endif
