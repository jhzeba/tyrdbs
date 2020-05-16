#pragma once


#include <gt/condition.h>
#include <tyrdbs/slice_writer.h>
#include <tyrdbs/iterator.h>
#include <tyrdbs/meta_node/modules.json.h>


namespace tyrtech::tyrdbs::meta_node::log {


class impl : private disallow_copy
{
private:
    using slice_writer_ptr =
            std::shared_ptr<tyrdbs::slice_writer>;

    using ushards_t =
            std::vector<slices_t>;

public:
    class context : private disallow_copy
    {
    public:
        ~context();

    public:
        context(context&& other) noexcept;
        context& operator=(context&& other) noexcept;

    private:
        using slice_writers_t =
                std::unordered_map<uint32_t, slice_writer_ptr>;

        using iterator_ptr =
                std::unique_ptr<tyrdbs::iterator>;

    private:
        struct writer
        {
            slice_writers_t slice_writers;
        };

        struct reader
        {
            iterator_ptr iterator;
            std::string_view value_part;
        };

    private:
        using writers_t =
                std::unordered_map<uint64_t, writer>;

        using readers_t =
                std::unordered_map<uint64_t, reader>;

    private:
        writers_t m_writers;
        readers_t m_readers;

        uint64_t m_next_handle{1};

    private:
        uint64_t next_handle();

        writers_t::iterator get_writer(uint64_t handle);
        readers_t::iterator get_reader(uint64_t handle);

    private:
        context(impl* impl);

    private:
        friend class impl;
    };

public:
    context create_context(const std::shared_ptr<io::socket>& remote);

public:
    void update(const update::request_parser_t& request,
                net::socket_channel* channel,
                context* ctx);

    void commit(const commit::request_parser_t& request,
                net::socket_channel* channel,
                context* ctx);

    void rollback(const rollback::request_parser_t& request,
                  net::socket_channel* channel,
                  context* ctx);

    void fetch(const fetch::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx);

    void abort(const abort::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx);

public:
    impl(const std::string_view& path,
         uint32_t max_slices,
         uint32_t merge_threads,
         uint32_t ushards);

private:
    ushards_t m_ushards;

    uint32_t m_max_slices{0};
    uint32_t m_slice_count{0};
    gt::condition m_slice_count_cond;

    uint64_t m_next_tid{1};

    std::vector<bool> m_ushard_locks;

private:
    bool fetch_entries(context::reader* r, message::builder* builder);
    void update_data(const message::parser* p, uint16_t off, context::writer* w);

    slice_writer_ptr new_slice_writer();

    void compact_if_needed(uint32_t ushard);
    void merge_thread();
};

}
