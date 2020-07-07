#pragma once


#include <tyrdbs/node.h>


namespace tyrtech::tyrdbs {


class node_writer : private disallow_copy, disallow_move
{
public:
    static constexpr uint32_t max_keys{1024};
    static constexpr int32_t compression_level{4};

public:
    int32_t add(const std::string_view& key,
                const std::string_view& value,
                bool eor,
                bool deleted,
                uint64_t meta,
                bool no_split);

    uint32_t flush(char* sink, uint32_t sink_size);

private:
    node::data_t m_keys;
    node::data_t m_values;

    node::entry m_entries[max_keys];

    uint16_t m_available_size{node::page_size};

    uint16_t m_keys_size{0};
    uint16_t m_values_size{0};

    uint16_t m_key_count{0};

    uint16_t m_key_size{static_cast<uint16_t>(-1)};
    uint16_t m_value_size{static_cast<uint16_t>(-1)};

private:
    uint32_t compress_block(char* source,
                            uint32_t source_size,
                            char* sink,
                            uint32_t* sink_size,
                            uint32_t type_size);
};

}
