#pragma once


#include <common/exception.h>
#include <tyrdbs/writer.h>
#include <tyrdbs/slice.h>
#include <tyrdbs/node_writer.h>
#include <tyrdbs/key_buffer.h>


namespace tyrtech::tyrdbs {


class slice_writer : private disallow_copy, disallow_move
{
public:
    DEFINE_EXCEPTION(runtime_error_exception, invalid_data_exception);

public:
    void add(iterator* it, bool compact);
    void add(const std::string_view& key,
             std::string_view value,
             bool eor,
             bool deleted);

    void flush();
    void commit();

    uint64_t size() const;
    uint64_t key_count() const;

public:
    slice_writer(std::shared_ptr<tyrdbs::writer> writer);
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
    using writer_ptr =
            std::shared_ptr<tyrdbs::writer>;

private:
    key_buffer m_first_key;
    key_buffer m_last_key;

    bool m_last_eor{false};
    bool m_commited{false};

    writer_ptr m_writer;

    node_writer m_node;
    index_writer m_index{this};

    slice::header m_header;

private:
    void add(const std::string_view& key,
             std::string_view value,
             bool eor,
             bool deleted,
             uint64_t tid);

    bool check(const std::string_view& key,
               const std::string_view& value,
               bool eor,
               bool deleted);

    uint64_t store(node_writer* node, bool is_leaf);
};

}
