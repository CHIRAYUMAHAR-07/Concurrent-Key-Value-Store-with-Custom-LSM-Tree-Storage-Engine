#pragma once
#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <map>
#include <string>
#include <optional>
#include <shared_mutex>
#include <atomic>
#include <vector>
#include <cstdint>

namespace kvstore
{

    using SeqNum = uint64_t;

    struct ValEntry
    {
        std::string value;
        bool deleted;
        SeqNum seq;
    };

    struct KVPair
    {
        std::string key, value;
        bool deleted;
        SeqNum seq;
    };

    class MemTable
    {
    public:
        static constexpr size_t DEFAULT_SIZE = 4 * 1024 * 1024;

        explicit MemTable(size_t max_bytes = DEFAULT_SIZE)
            : max_size_(max_bytes), cur_size_(0), frozen_(false), seq_(0) {}

        bool Put(const std::string &k, const std::string &v)
        {
            if (frozen_.load(std::memory_order_acquire))
                return false;
            std::unique_lock lk(mu_);
            SeqNum s = ++seq_;
            auto it = table_.find(k);
            if (it != table_.end())
                cur_size_ -= it->first.size() + it->second.value.size();
            table_[k] = {v, false, s};
            cur_size_ += k.size() + v.size() + sizeof(ValEntry);
            return true;
        }

        bool Delete(const std::string &k)
        {
            if (frozen_.load(std::memory_order_acquire))
                return false;
            std::unique_lock lk(mu_);
            SeqNum s = ++seq_;
            table_[k] = {"", true, s};
            cur_size_ += k.size() + sizeof(ValEntry);
            return true;
        }

        std::optional<ValEntry> Get(const std::string &k) const
        {
            std::shared_lock lk(mu_);
            auto it = table_.find(k);
            if (it == table_.end())
                return std::nullopt;
            return it->second;
        }

        std::vector<KVPair> Scan(const std::string &start,
                                 const std::string &end,
                                 size_t limit = SIZE_MAX) const
        {
            std::shared_lock lk(mu_);
            std::vector<KVPair> out;
            auto it = start.empty() ? table_.begin() : table_.lower_bound(start);
            auto eit = end.empty() ? table_.end() : table_.lower_bound(end);
            for (; it != eit && out.size() < limit; ++it)
                if (!it->second.deleted)
                    out.push_back({it->first, it->second.value,
                                   it->second.deleted, it->second.seq});
            return out;
        }

        std::vector<KVPair> SortedEntries() const
        {
            std::shared_lock lk(mu_);
            std::vector<KVPair> out;
            out.reserve(table_.size());
            for (auto &[k, v] : table_)
                out.push_back({k, v.value, v.deleted, v.seq});
            return out;
        }

        bool ShouldFlush() const { return cur_size_.load() >= max_size_; }
        size_t SizeBytes() const { return cur_size_.load(); }
        size_t Count() const
        {
            std::shared_lock lk(mu_);
            return table_.size();
        }
        SeqNum LastSeq() const { return seq_.load(); }

        void Freeze() { frozen_.store(true, std::memory_order_release); }
        bool IsFrozen() const { return frozen_.load(std::memory_order_acquire); }

    private:
        mutable std::shared_mutex mu_;
        std::map<std::string, ValEntry> table_;
        size_t max_size_;
        std::atomic<size_t> cur_size_;
        std::atomic<bool> frozen_;
        std::atomic<SeqNum> seq_;
    };

}

#endif
