#include <common/branch_prediction.h>
#include <common/exception.h>
#include <tyrdbs/node_writer.h>

#include <blosc.h>

#include <memory>
#include <cstring>


namespace tyrtech::tyrdbs {


int32_t node_writer::add(const std::string_view& key,
                         const std::string_view& value,
                         bool eor,
                         bool deleted,
                         uint64_t meta,
                         bool no_split)
{
    assert(likely(key.size() != 0));
    assert(likely(key.size() <= node::max_key_size));

    if (m_key_count == max_keys)
    {
        return -1;
    }

    uint16_t required_size{0};

    required_size += sizeof(node::entry);
    required_size += key.size();

    if (no_split == true)
    {
        required_size += value.size();
    }

    if (m_available_size < required_size)
    {
        return -1;
    }

    m_available_size -= sizeof(node::entry);
    m_available_size -= key.size();

    std::memcpy(m_keys.data() + m_keys_size, key.data(), key.size());
    m_keys_size += key.size();

    auto partial_value_size = std::min(static_cast<size_t>(m_available_size),
                                       value.size());

    m_available_size -= partial_value_size;

    std::memcpy(m_values.data() + m_values_size, value.data(), partial_value_size);
    m_values_size += partial_value_size;

    node::entry& e = m_entries[m_key_count];

    e.key_end_offset = m_keys_size;
    e.value_end_offset = m_values_size;
    e.eor = eor && value.size() == partial_value_size;
    e.meta = meta;
    e.deleted = deleted;

    m_key_count++;

    if (m_key_size == static_cast<uint16_t>(-1))
    {
        m_key_size = key.size();
    }

    if (m_key_size != key.size())
    {
        m_key_size = 0;
    }

    if (m_value_size == static_cast<uint16_t>(-1))
    {
        m_value_size = partial_value_size;
    }

    if (m_value_size != partial_value_size)
    {
        m_value_size = 0;
    }

    return partial_value_size;
}

uint32_t node_writer::flush(char* sink, uint32_t sink_size)
{
    assert(likely(sink_size >= node::page_size + 128));
    uint32_t _sink_size = sink_size;

    {
        uint16_t* key_count = reinterpret_cast<uint16_t*>(sink);
        *key_count = m_key_count;

        sink += sizeof(uint16_t);
        sink_size -= sizeof(uint16_t);
    }

    sink += compress_block(reinterpret_cast<char*>(m_entries),
                           sizeof(node::entry) * m_key_count,
                           sink,
                           &sink_size,
                           sizeof(node::entry));

    sink += compress_block(m_keys.data(),
                           m_keys_size,
                           sink,
                           &sink_size,
                           m_key_size);

    sink += compress_block(m_values.data(),
                           m_values_size,
                           sink,
                           &sink_size,
                           m_value_size);

    m_available_size = node::page_size;
    m_keys_size = 0;
    m_values_size = 0;
    m_key_count = 0;
    m_key_size = static_cast<uint16_t>(-1);
    m_value_size = static_cast<uint16_t>(-1);

    return _sink_size - sink_size;
}

uint32_t node_writer::compress_block(char* source,
                                     uint32_t source_size,
                                     char* sink,
                                     uint32_t* sink_size,
                                     uint32_t type_size)
{
    uint16_t* size = reinterpret_cast<uint16_t*>(sink);

    sink += sizeof(uint16_t);
    *sink_size -= sizeof(uint16_t);

    auto res = blosc_compress_ctx(compression_level,
                                  (type_size != 0) ? BLOSC_SHUFFLE : BLOSC_NOSHUFFLE,
                                  (type_size != 0) ? type_size : 1,
                                  source_size,
                                  source,
                                  sink,
                                  *sink_size,
                                  BLOSC_LZ4_COMPNAME,
                                  0, 0);
    assert(res > 0);

    *sink_size -= res;
    *size = res;

    return sizeof(uint16_t) + res;
}

}
