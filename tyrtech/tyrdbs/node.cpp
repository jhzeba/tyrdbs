#include <common/branch_prediction.h>
#include <common/exception.h>
#include <tyrdbs/node.h>

#include <blosc.h>
#include <cassert>


namespace tyrtech::tyrdbs {


void node::load(const char* source, uint32_t source_size)
{
    assert(likely(m_key_count == static_cast<uint16_t>(-1)));
    uint32_t output_size{0};

    {
        m_key_count = *reinterpret_cast<const uint16_t*>(source);
        source += sizeof(uint16_t);
    }

    m_entries = reinterpret_cast<entry*>(m_data.data() + output_size);
    source += decompress_block(source, &output_size);

    m_keys = m_data.data() + output_size;
    source += decompress_block(source, &output_size);

    m_values = m_data.data() + output_size;
    source += decompress_block(source, &output_size);
}

uint64_t node::get_next() const
{
    return m_next_node;
}

void node::set_next(uint64_t next_node)
{
    assert(likely(next_node != 0));
    m_next_node = next_node;
}

uint16_t node::key_count() const
{
    return m_key_count;
}

std::string_view node::key_at(uint16_t ndx) const
{
    assert(ndx < m_key_count);

    uint16_t start_off = 0;
    uint16_t end_off = m_entries[ndx].key_end_offset;

    if (ndx != 0)
    {
        start_off = m_entries[ndx-1].key_end_offset;
    }

    return std::string_view(m_keys + start_off,
                            end_off - start_off);
}

std::string_view node::value_at(uint16_t ndx) const
{
    assert(ndx < m_key_count);

    uint16_t start_off = 0;
    uint16_t end_off = m_entries[ndx].value_end_offset;

    if (ndx != 0)
    {
        start_off = m_entries[ndx-1].value_end_offset;
    }

    return std::string_view(m_values + start_off,
                            end_off - start_off);
}

bool node::eor_at(uint16_t ndx) const
{
    assert(ndx < m_key_count);
    return m_entries[ndx].eor;
}

bool node::deleted_at(uint16_t ndx) const
{
    assert(ndx < m_key_count);
    return m_entries[ndx].deleted;
}

uint64_t node::meta_at(uint16_t ndx) const
{
    assert(ndx < m_key_count);
    return m_entries[ndx].meta;
}

uint16_t node::lower_bound(const std::string_view& key) const
{
    uint16_t start = 0;
    uint16_t end = key_count();

    while (end - start > 8)
    {
        uint16_t ndx = ((end - start) >> 1) + start;

        int32_t cmp = key.compare(key_at(ndx));

        if (cmp <= 0)
        {
            end = ndx;
        }
        else
        {
            start = ndx;
        }
    }

    for (uint16_t ndx = start; ndx < end; ndx++)
    {
        if (key.compare(key_at(ndx)) <= 0)
        {
            return ndx;
        }
    }

    return end;
}

uint32_t node::decompress_block(const char* source, uint32_t* output_size)
{
    uint16_t size = *reinterpret_cast<const uint16_t*>(source);
    source += sizeof(uint16_t);

    char* sink = m_data.data() + *output_size;

    auto res = blosc_decompress_ctx(source,
                                    sink,
                                    m_data.size() - *output_size,
                                    0);

    if (res < 0)
    {
        throw runtime_error_exception("unable to decompress node");
    }

    *output_size += res;

    return sizeof(uint16_t) + size;
}

}
