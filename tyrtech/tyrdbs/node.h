#pragma once


#include <common/disallow_copy.h>
#include <common/disallow_move.h>

#include <string>
#include <array>


namespace tyrtech::tyrdbs {


class node : private disallow_copy, disallow_move
{
public:
    static constexpr uint32_t page_size{16384};
    static constexpr uint32_t max_key_size{1024};

public:
    void load(const char* source, uint32_t source_size);

    uint64_t get_next() const;
    void set_next(uint64_t next_node);

    uint16_t key_count() const;

    std::string_view key_at(uint16_t ndx) const;
    std::string_view value_at(uint16_t ndx) const;
    bool eor_at(uint16_t ndx) const;
    bool deleted_at(uint16_t ndx) const;
    uint64_t meta_at(uint16_t ndx) const;

    uint16_t lower_bound(const std::string_view& key) const;

private:
    struct entry
    {
        uint16_t key_end_offset : 15;
        uint16_t eor : 1;
        uint16_t value_end_offset : 15;
        uint16_t deleted : 1;
        uint64_t meta;
    } __attribute__ ((packed));

private:
    using data_t =
            std::array<char, page_size>;

private:
    data_t m_data;

    const entry* m_entries{nullptr};
    const char* m_keys{nullptr};
    const char* m_values{nullptr};

    uint16_t m_key_count{static_cast<uint16_t>(-1)};
    uint64_t m_next_node{static_cast<uint64_t>(-1)};

private:
    uint32_t decompress_block(const char* source, uint32_t* output_size);

private:
    friend class node_writer;
} __attribute__ ((packed));

}
