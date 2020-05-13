#pragma once


#include <common/buffered_writer.h>
#include <io/file_writer.h>
#include <tyrdbs/slice.h>
#include <tyrdbs/node_writer.h>
#include <tyrdbs/key_buffer.h>


namespace tyrtech::tyrdbs {


class slice_writer : private disallow_copy, disallow_move
{
public:
    DEFINE_EXCEPTION(runtime_error, invalid_data_error);

public:
    static constexpr uint64_t max_idx{0x1000000000000ULL};

public:
    void add(iterator* it, bool compact);
    void add(const std::string_view& key,
             std::string_view value,
             bool eor,
             bool deleted,
             uint64_t idx);

    void flush();
    uint64_t commit();

public:
    template<typename... Arguments>
    slice_writer(Arguments&&... arguments)
      : m_slice_ndx(-1)//storage::new_cache_id())
      , m_file(io::file::create(std::forward<Arguments>(arguments)...))
    {
    }

    ~slice_writer();

private:
    class index_writer : private disallow_copy, disallow_move
    {
    public:
        void add(const std::string_view& min_key,
                 const std::string_view& max_key,
                 uint64_t location);

        uint64_t flush();

    public:
        index_writer(slice_writer* writer);

    private:
        using index_writer_ptr =
                std::unique_ptr<index_writer>;

    private:
        node_writer m_node;

        key_buffer m_first_key;
        key_buffer m_last_key;

        index_writer_ptr m_higher_level;

        slice_writer* m_writer{nullptr};
    };

private:
    using buffer_t =
            std::array<char, node::page_size>;

    using writer_t =
            buffered_writer<buffer_t, io::file_writer>;

private:
    uint64_t m_slice_ndx{static_cast<uint64_t>(-1)};

    key_buffer m_first_key;
    key_buffer m_last_key;

    bool m_last_eor{true};

    bool m_commited{false};

    buffer_t m_buffer;
    io::file m_file;

    io::file_writer m_file_writer{&m_file, 0};
    writer_t m_writer{&m_buffer, &m_file_writer};

    node_writer m_node;
    index_writer m_index{this};

    slice::header m_header;

    std::shared_ptr<node> m_last_node;

private:
    bool check(const std::string_view& key,
               const std::string_view& value,
               bool eor,
               bool deleted);

    uint64_t store(node_writer* node, bool is_leaf);
};

}
