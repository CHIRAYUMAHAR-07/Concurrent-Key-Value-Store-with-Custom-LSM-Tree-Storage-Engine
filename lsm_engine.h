#pragma once
#ifndef LSM_ENGINE_H
#define LSM_ENGINE_H
#include "memtable.h"
#include "sstable.h"
#include "wal.h"
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <sstream>
#include <iomanip>
#include <optional>
#include <map>
#include <algorithm>

namespace kvstore
{
    namespace fs = std::filesystem;

    struct Config
    {
        std::string data_dir = "./kvdata";
        size_t memtable_max = 4 * 1024 * 1024; // 4 MB
        int l0_compact_threshold = 4;
        int max_levels = 7;
        bool sync_writes = true;
    };

    struct EngineStats
    {
        std::atomic<uint64_t> writes{0}, reads{0}, dels{0};
        std::atomic<uint64_t> compactions{0}, flushes{0};
        std::atomic<uint64_t> bloom_skips{0}, disk_reads{0};
    };

    class LSMEngine
    {
    public:
        explicit LSMEngine(const Config &cfg = Config{})
            : cfg_(cfg), shutdown_(false), seq_(1)
        {
            fs::create_directories(cfg_.data_dir);
            wal_ = std::make_unique<WAL>(cfg_.data_dir + "/wal.log");
            active_ = std::make_shared<MemTable>(cfg_.memtable_max);
            recoverWAL();
            loadSSTables();
            compact_thread_ = std::thread(&LSMEngine::compactLoop, this);
        }

        ~LSMEngine()
        {
            shutdown_.store(true, std::memory_order_release);
            cv_.notify_all();
            if (compact_thread_.joinable())
                compact_thread_.join();
            flushActive(true);
        }

        bool Put(const std::string &k, const std::string &v)
        {
            if (k.empty() || k.size() > 4096)
                return false;
            uint64_t s = seq_.fetch_add(1, std::memory_order_relaxed);
            if (cfg_.sync_writes)
            {
                wal_->AppendPut(s, k, v);
                wal_->Sync();
            }
            {
                std::shared_lock lk(mu_);
                active_->Put(k, v);
            }
            stats_.writes++;
            if (active_->ShouldFlush())
                triggerFlush();
            return true;
        }

        std::optional<std::string> Get(const std::string &k)
        {
            stats_.reads++;
            {
                std::shared_lock lk(mu_);
                if (auto r = active_->Get(k))
                {
                    return r->deleted ? std::nullopt : std::optional<std::string>(r->value);
                }
                for (auto it = imm_.rbegin(); it != imm_.rend(); ++it)
                {
                    if (auto r = (*it)->Get(k))
                        return r->deleted ? std::nullopt : std::optional<std::string>(r->value);
                }
            }
            std::shared_lock lk(sst_mu_);
            for (auto &level : levels_)
            {
                for (auto it = level.rbegin(); it != level.rend(); ++it)
                {
                    if (!(*it)->Overlaps(k))
                        continue;
                    stats_.disk_reads++;
                    auto r = (*it)->Get(k);
                    stats_.bloom_skips += (*it)->BloomSaves();
                    if (r)
                        return r->deleted ? std::nullopt : std::optional<std::string>(r->value);
                }
            }
            return std::nullopt;
        }

        bool Delete(const std::string &k)
        {
            uint64_t s = seq_.fetch_add(1, std::memory_order_relaxed);
            if (cfg_.sync_writes)
            {
                wal_->AppendDel(s, k);
                wal_->Sync();
            }
            std::shared_lock lk(mu_);
            stats_.dels++;
            return active_->Delete(k);
        }

        std::vector<KVPair> Scan(const std::string &start, const std::string &end, size_t lim = 1000)
        {
            std::map<std::string, KVPair> merged;
            auto add = [&](const KVPair &e)
            {
                auto it = merged.find(e.key);
                if (it == merged.end() || e.seq > it->second.seq)
                    merged[e.key] = e;
            };
            {
                std::shared_lock lk(mu_);
                for (auto &e : active_->Scan(start, end, lim))
                    add(e);
                for (auto &im : imm_)
                    for (auto &e : im->Scan(start, end, lim))
                        add(e);
            }
            {
                std::shared_lock lk(sst_mu_);
                for (auto &lv : levels_)
                    for (auto &r : lv)
                        for (auto &e : r->Scan(start, end, lim))
                            add(e);
            }
            std::vector<KVPair> out;
            for (auto &[k, e] : merged)
                if (!e.deleted)
                {
                    out.push_back(e);
                    if (out.size() >= lim)
                        break;
                }
            return out;
        }

        void ForceFlush()
        {
            triggerFlush();
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
        void ForceCompact()
        {
            cv_.notify_one();
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        EngineStats &Stats() { return stats_; }

        size_t SSTCount() const
        {
            std::shared_lock lk(sst_mu_);
            size_t n = 0;
            for (auto &lv : levels_)
                n += lv.size();
            return n;
        }

        double BloomSaveRate() const
        {
            uint64_t s = stats_.bloom_skips.load(), d = stats_.disk_reads.load();
            return (s + d) == 0 ? 0.0 : double(s) / double(s + d);
        }

    private:
        void triggerFlush()
        {
            std::unique_lock lk(mu_);
            if (!active_->ShouldFlush())
                return;
            active_->Freeze();
            imm_.push_back(active_);
            active_ = std::make_shared<MemTable>(cfg_.memtable_max);
            lk.unlock();
            cv_.notify_one();
        }

        void flushImm(std::shared_ptr<MemTable> imm)
        {
            auto entries = imm->SortedEntries();
            if (entries.empty())
                return;
            std::string path = sstPath(0);
            SSTWriter w(path);
            auto meta = w.Write(entries);
            meta.level = 0;
            wal_->Checkpoint(imm->LastSeq());
            wal_->Sync();
            auto reader = std::make_shared<SSTReader>(path);
            std::unique_lock lk(sst_mu_);
            ensureLevels(0);
            levels_[0].push_back(reader);
            stats_.flushes++;
        }

        void flushActive(bool)
        {
            if (active_->Count() == 0)
                return;
            active_->Freeze();
            flushImm(active_);
        }

        void compactLoop()
        {
            while (!shutdown_.load(std::memory_order_acquire))
            {
                std::unique_lock lk(cv_mu_);
                cv_.wait_for(lk, std::chrono::milliseconds(200), [this]
                             { return shutdown_.load() || !imm_.empty() || needsCompact(); });
                if (shutdown_.load())
                    break;

                while (true)
                {
                    std::shared_ptr<MemTable> im;
                    {
                        std::unique_lock ml(mu_);
                        if (imm_.empty())
                            break;
                        im = imm_.front();
                        imm_.erase(imm_.begin());
                    }
                    flushImm(im);
                }

                if (needsCompact())
                    runCompact();
            }
        }

        bool needsCompact() const
        {
            std::shared_lock lk(sst_mu_);
            return !levels_.empty() && int(levels_[0].size()) >= cfg_.l0_compact_threshold;
        }

        void runCompact()
        {
            std::vector<KVPair> all;
            int src = 0;
            {
                std::shared_lock lk(sst_mu_);
                if (src >= int(levels_.size()))
                    return;
                for (auto &r : levels_[src])
                    for (auto &e : r->Scan("", ""))
                        all.push_back(e);
            }
            if (all.empty())
                return;

            std::sort(all.begin(), all.end(), [](auto &a, auto &b)
                      { return a.key < b.key || (a.key == b.key && a.seq > b.seq); });
            std::vector<KVPair> dedup;
            std::string last;
            for (auto &e : all)
                if (e.key != last)
                {
                    dedup.push_back(e);
                    last = e.key;
                }

            if (src == cfg_.max_levels - 2)
                dedup.erase(std::remove_if(dedup.begin(), dedup.end(), [](auto &e)
                                           { return e.deleted; }),
                            dedup.end());

            if (dedup.empty())
                return;

            int dst = src + 1;
            std::string path = sstPath(dst);
            SSTWriter w(path);
            w.Write(dedup);
            auto newReader = std::make_shared<SSTReader>(path);

            {
                std::unique_lock lk(sst_mu_);
                levels_[src].clear();
                ensureLevels(dst);
                levels_[dst].push_back(newReader);
            }
            stats_.compactions++;
        }

        void recoverWAL()
        {
            WAL::Replay(cfg_.data_dir + "/wal.log", 0, [this](const WALRecord &r)
                        {
            if (r.type==WALType::PUT) active_->Put(r.key, r.value);
            else if (r.type==WALType::DEL) active_->Delete(r.key); });
        }

        void loadSSTables()
        {
            for (auto &entry : fs::directory_iterator(cfg_.data_dir))
            {
                if (entry.path().extension() != ".sst")
                    continue;
                auto stem = entry.path().stem().string();
                int lv = 0;
                if (!stem.empty() && stem[0] == 'L')
                {
                    try
                    {
                        lv = std::stoi(stem.substr(1, stem.find('_') - 1));
                    }
                    catch (...)
                    {
                        lv = 0;
                    }
                }
                try
                {
                    auto r = std::make_shared<SSTReader>(entry.path().string());
                    ensureLevels(lv);
                    levels_[lv].push_back(r);
                }
                catch (...)
                {
                }
            }
        }

        std::string sstPath(int level)
        {
            uint64_t s = seq_.fetch_add(1, std::memory_order_relaxed);
            std::ostringstream ss;
            ss << cfg_.data_dir << "/L" << level
               << "_" << std::setw(12) << std::setfill('0') << s << ".sst";
            return ss.str();
        }

        void ensureLevels(int lv)
        {
            while (int(levels_.size()) <= lv)
                levels_.emplace_back();
        }

        Config cfg_;
        std::atomic<bool> shutdown_;
        std::atomic<uint64_t> seq_;
        mutable std::shared_mutex mu_;
        std::shared_ptr<MemTable> active_;
        std::vector<std::shared_ptr<MemTable>> imm_;
        mutable std::shared_mutex sst_mu_;
        std::vector<std::vector<std::shared_ptr<SSTReader>>> levels_;
        std::unique_ptr<WAL> wal_;
        std::thread compact_thread_;
        std::mutex cv_mu_;
        std::condition_variable cv_;
        EngineStats stats_;
    };

}
#endif