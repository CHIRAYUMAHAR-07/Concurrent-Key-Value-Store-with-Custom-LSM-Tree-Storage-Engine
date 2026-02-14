#include "bridge.h"
#include "lsm_engine.h"
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

static kvstore::LSMEngine *eng(LSMHandle h)
{
    return reinterpret_cast<kvstore::LSMEngine *>(h);
}

extern "C"
{

    LSMHandle lsm_create(const char *data_dir)
    {
        try
        {
            kvstore::Config cfg;
            if (data_dir && *data_dir)
                cfg.data_dir = data_dir;
            cfg.sync_writes = true;
            return new kvstore::LSMEngine(cfg);
        }
        catch (...)
        {
            return nullptr;
        }
    }

    void lsm_destroy(LSMHandle h) { delete eng(h); }

    int lsm_put(LSMHandle h, const char *key, const char *val, int vlen)
    {
        if (!h || !key)
            return -1;
        try
        {
            return eng(h)->Put(std::string(key), std::string(val, size_t(vlen))) ? 0 : -1;
        }
        catch (...)
        {
            return -1;
        }
    }

    char *lsm_get(LSMHandle h, const char *key, int *out_len)
    {
        if (!h || !key || !out_len)
            return nullptr;
        *out_len = 0;
        try
        {
            auto r = eng(h)->Get(std::string(key));
            if (!r)
                return nullptr;
            char *buf = (char *)std::malloc(r->size() + 1);
            if (!buf)
                return nullptr;
            std::memcpy(buf, r->data(), r->size());
            buf[r->size()] = '\0';
            *out_len = (int)r->size();
            return buf;
        }
        catch (...)
        {
            return nullptr;
        }
    }

    int lsm_delete(LSMHandle h, const char *key)
    {
        if (!h || !key)
            return -1;
        try
        {
            return eng(h)->Delete(std::string(key)) ? 0 : -1;
        }
        catch (...)
        {
            return -1;
        }
    }

    char *lsm_scan(LSMHandle h, const char *start, const char *end, int limit, int *out_count)
    {
        if (!h || !out_count)
            return nullptr;
        *out_count = 0;
        try
        {
            auto entries = eng(h)->Scan(
                start ? std::string(start) : "",
                end ? std::string(end) : "",
                size_t(limit));
            *out_count = (int)entries.size();
            if (entries.empty())
                return nullptr;

            size_t total = 4;
            for (auto &e : entries)
                total += 4 + e.key.size() + 4 + e.value.size();
            char *buf = (char *)std::malloc(total);
            if (!buf)
                return nullptr;

            size_t off = 0;
            auto w32 = [&](uint32_t v)
            {
            for (int i=0;i<4;i++) buf[off++] = (v>>(i*8))&0xFF; };
            w32(uint32_t(entries.size()));
            for (auto &e : entries)
            {
                w32(uint32_t(e.key.size()));
                std::memcpy(buf + off, e.key.data(), e.key.size());
                off += e.key.size();
                w32(uint32_t(e.value.size()));
                std::memcpy(buf + off, e.value.data(), e.value.size());
                off += e.value.size();
            }
            return buf;
        }
        catch (...)
        {
            *out_count = 0;
            return nullptr;
        }
    }

    char *lsm_stats(LSMHandle h)
    {
        if (!h)
            return nullptr;
        try
        {
            auto &s = eng(h)->Stats();
            std::ostringstream ss;
            ss << "{\"writes\":" << s.writes.load()
               << ",\"reads\":" << s.reads.load()
               << ",\"deletes\":" << s.dels.load()
               << ",\"compactions\":" << s.compactions.load()
               << ",\"flushes\":" << s.flushes.load()
               << ",\"bloom_saves\":" << s.bloom_skips.load()
               << ",\"disk_reads\":" << s.disk_reads.load()
               << ",\"bloom_save_rate\":" << eng(h)->BloomSaveRate()
               << ",\"sst_count\":" << eng(h)->SSTCount()
               << "}";
            std::string str = ss.str();
            char *buf = (char *)std::malloc(str.size() + 1);
            if (!buf)
                return nullptr;
            std::memcpy(buf, str.data(), str.size() + 1);
            return buf;
        }
        catch (...)
        {
            return nullptr;
        }
    }

    void lsm_free(void *p) { std::free(p); }

    void lsm_flush(LSMHandle h)
    {
        if (h)
            eng(h)->ForceFlush();
    }

    int lsm_sst_count(LSMHandle h)
    {
        if (!h)
            return 0;
        return (int)eng(h)->SSTCount();
    }
}
