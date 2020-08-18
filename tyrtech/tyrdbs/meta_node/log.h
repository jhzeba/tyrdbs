#pragma once


#include <common/dynamic_buffer.h>
#include <gt/condition.h>
#include <tyrdbs/slice_writer.h>
#include <tyrdbs/meta_node/ushard.h>
#include <tyrdbs/meta_node/modules.json.h>
#include <tyrdbs/meta_node/block.json.h>

#include <queue>


namespace tyrtech::tyrdbs::meta_node::log {


class impl : private disallow_copy
{
public:
    struct context : private disallow_copy
    {
        net::socket_channel* channel{nullptr};

        context(net::socket_channel* channel);
    };

public:
    context create_context(net::socket_channel* channel);

public:
    void fetch(const fetch::request_parser_t& request, context* ctx);
    void watch(const watch::request_parser_t& request, context* ctx);
    void update(const update::request_parser_t& request, context* ctx);

public:
    impl(const std::string_view& path,
         uint32_t merge_threads,
         uint32_t ushards,
         uint32_t max_slices);

private:
    using buffer_t =
            std::array<char, net::socket_channel::buffer_size>;

    using ushards_t =
            std::vector<ushard>;

    using writer_ptr =
        std::unique_ptr<slice_writer>;

    using writers_t =
            std::unordered_map<uint16_t, writer_ptr>;

    using slices_t =
            std::unordered_map<uint16_t, slice_ptr>;

    using blocks_t =
            std::vector<dynamic_buffer>;

    using blocks_ptr =
            std::shared_ptr<blocks_t>;

    using transaction_t =
            std::tuple<slices_t, blocks_ptr>;

    using transaction_log_t =
            std::queue<std::tuple<uint64_t, blocks_ptr>>;

    using transaction_log_map_t =
            std::unordered_map<gt::context_t, transaction_log_t*>;

private:
    std::string_view m_path;

    uint64_t m_next_cache_id{1};
    uint64_t m_next_tid{1};

    uint32_t m_max_slices{16384};

    uint32_t m_slice_count{0};
    gt::condition m_slice_count_cond;

    ushards_t m_ushards;

    transaction_log_map_t m_transaction_log_map;
    gt::condition m_transaction_log_condition;

private:
    bool load_block(const block_parser& block, writers_t* writers);
    transaction_t process_transaction(net::socket_channel* channel);

    void register_transaction_log(transaction_log_t* transaction_log);
    void unregister_transaction_log();

    void push_transaction(uint64_t tid, blocks_ptr blocks);

    void send_keys(uint16_t ushard_id, net::socket_channel* channel);
};

}
