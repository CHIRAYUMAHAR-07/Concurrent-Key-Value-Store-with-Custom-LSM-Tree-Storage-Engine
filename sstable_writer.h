#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <vector>
#include <string>
#include <algorithm>
#include <cassert>

static uint32_t crc32_table[256];
static bool crc32_init_done = false;

static void crc32_init()
{
    if (crc32_init_done)
        return;
    for (uint32_t i = 0; i < 256; i++)
    {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_init_done = true;
}

static uint32_t crc32(const uint8_t *data, size_t len)
{
    crc32_init();
    uint32_t c = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++)
        c = crc32_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFF;
}

static size_t encode_varint(uint64_t v, uint8_t *out)
{
    size_t n = 0;
    do
    {
        out[n] = v & 0x7F;
        v >>= 7;
        if (v)
            out[n] |= 0x80;
        n++;
    } while (v);
    return n;
}

struct KVEntry
{
    std::string key;
    std::string value;
    bool is_tombstone;
    uint64_t sequence_num;
};

static const uint64_t SSTABLE_MAGIC = 0x4C534D53535441BULL;
static const uint64_t SSTABLE_MAGIC_END = 0x454E44464F4F5455ULL;
static const int INDEX_INTERVAL = 16;

class SSTableWriter
{
public:
    struct IndexEntry
    {
        std::string key;
        uint64_t offset;
    };

    static uint8_t *write(const KVEntry *entries, size_t count,
                          const uint8_t *bloom_bytes, size_t bloom_len,
                          size_t *out_len)
    {
        std::vector<uint8_t> buf;
        buf.reserve(count * 64 + bloom_len + 512);

        append_u64(buf, SSTABLE_MAGIC);
        uint64_t data_offset = buf.size();
        std::vector<IndexEntry> index_entries;

        for (size_t i = 0; i < count; i++)
        {
            if (i % INDEX_INTERVAL == 0)
            {
                index_entries.push_back({entries[i].key, (uint64_t)buf.size()});
            }

            size_t entry_start = buf.size();
            uint8_t tmp[10];
            size_t kl = encode_varint(entries[i].key.size(), tmp);
            buf.insert(buf.end(), tmp, tmp + kl);
            buf.insert(buf.end(), entries[i].key.begin(), entries[i].key.end());
            size_t vl = encode_varint(entries[i].value.size(), tmp);
            buf.insert(buf.end(), tmp, tmp + vl);
            buf.insert(buf.end(), entries[i].value.begin(), entries[i].value.end());
            uint8_t flags = entries[i].is_tombstone ? 0x01 : 0x00;
            buf.push_back(flags);
            append_u64(buf, entries[i].sequence_num);
            uint32_t c = crc32(buf.data() + entry_start, buf.size() - entry_start);
            append_u32(buf, c);
        }
        uint64_t data_len = buf.size() - data_offset;

        uint64_t index_offset = buf.size();
        append_u64(buf, (uint64_t)index_entries.size());
        for (auto &ie : index_entries)
        {
            uint8_t tmp[10];
            size_t kl = encode_varint(ie.key.size(), tmp);
            buf.insert(buf.end(), tmp, tmp + kl);
            buf.insert(buf.end(), ie.key.begin(), ie.key.end());
            append_u64(buf, ie.offset);
        }

        uint64_t bloom_offset = buf.size();
        append_u64(buf, (uint64_t)bloom_len);
        buf.insert(buf.end(), bloom_bytes, bloom_bytes + bloom_len);

        size_t footer_start = buf.size();
        append_u64(buf, data_offset);
        append_u64(buf, data_len);
        append_u64(buf, index_offset);
        append_u64(buf, bloom_offset);
        append_u64(buf, SSTABLE_MAGIC_END);
        uint32_t fc = crc32(buf.data() + footer_start, buf.size() - footer_start);
        append_u32(buf, fc);
        append_u32(buf, 0);

        *out_len = buf.size();
        uint8_t *out = new uint8_t[buf.size()];
        memcpy(out, buf.data(), buf.size());
        return out;
    }

    static void free_buffer(uint8_t *buf) { delete[] buf; }

private:
    static void append_u32(std::vector<uint8_t> &v, uint32_t x)
    {
        uint8_t b[4];
        b[0] = x & 0xFF;
        b[1] = (x >> 8) & 0xFF;
        b[2] = (x >> 16) & 0xFF;
        b[3] = (x >> 24) & 0xFF;
        v.insert(v.end(), b, b + 4);
    }
    static void append_u64(std::vector<uint8_t> &v, uint64_t x)
    {
        uint8_t b[8];
        for (int i = 0; i < 8; i++)
            b[i] = (x >> (8 * i)) & 0xFF;
        v.insert(v.end(), b, b + 8);
    }
};

extern "C"
{
    uint8_t *sstable_write(
        const uint8_t *entries_flat, size_t entries_flat_len,
        size_t entry_count,
        const uint8_t *bloom_bytes, size_t bloom_len,
        size_t *out_len)
    {
        std::vector<KVEntry> entries;
        entries.reserve(entry_count);
        const uint8_t *p = entries_flat;
        const uint8_t *end = entries_flat + entries_flat_len;

        for (size_t i = 0; i < entry_count && p < end; i++)
        {
            KVEntry e;
            if (p + 4 > end)
                break;

            uint32_t kl;
            memcpy(&kl, p, 4);
            p += 4;
            if (p + kl > end)
                break;
            e.key.assign((char *)p, kl);
            p += kl;

            if (p + 4 > end)
                break;
            uint32_t vl;
            memcpy(&vl, p, 4);
            p += 4;
            if (p + vl > end)
                break;
            e.value.assign((char *)p, vl);
            p += vl;

            if (p + 9 > end)
                break;
            e.is_tombstone = (*p & 0x01) != 0;
            p++;
            memcpy(&e.sequence_num, p, 8);
            p += 8;
            entries.push_back(std::move(e));
        }

        return SSTableWriter::write(
            entries.data(), entries.size(),
            bloom_bytes, bloom_len,
            out_len);
    }

    void sstable_free_buffer(uint8_t *buf)
    {
        SSTableWriter::free_buffer(buf);
    }
}
