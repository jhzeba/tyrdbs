#pragma once


#include <tyrdbs/meta_node/ushard.h>
#include <tyrdbs/meta_node/log_module.json.h>


namespace tyrtech::tyrdbs::meta_node::log {


class impl : private disallow_copy
{
public:
    class context : private disallow_copy
    {
    public:
        ~context();

    public:
        context(context&& other) noexcept;
        context& operator=(context&& other) noexcept;

    private:
        using slice_writer_ptr =
                std::shared_ptr<tyrdbs::slice_writer>;

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
    context create_context(const std::shared_ptr<io::channel>& remote);

public:
    void update(const update::request_parser_t& request,
                update::response_builder_t* response,
                context* ctx);

    void commit(const commit::request_parser_t& request,
                commit::response_builder_t* response,
                context* ctx);

    void rollback(const rollback::request_parser_t& request,
                  rollback::response_builder_t* response,
                  context* ctx);

    void fetch(const fetch::request_parser_t& request,
               fetch::response_builder_t* response,
               context* ctx);

    void abort(const abort::request_parser_t& request,
               abort::response_builder_t* response,
               context* ctx);

public:
    impl(const std::string_view& path, uint32_t ushards);

private:
    using ushard_ptr =
            std::shared_ptr<ushard>;

    using ushards_t =
            std::vector<ushard_ptr>;

private:
    ushards_t m_ushards;
    uint64_t m_next_tid{1};

private:
    bool fetch_entries(context::reader* r, message::builder* builder);
    void update_data(const message::parser* p, uint16_t off, context::writer* w);
};

}
