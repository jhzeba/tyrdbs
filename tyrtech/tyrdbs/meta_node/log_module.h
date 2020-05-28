#pragma once


#include <gt/condition.h>
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
    void update(const update::request_parser_t& request, context* ctx);

public:
    impl(const std::string_view& path,
         uint32_t merge_threads,
         uint32_t ushards,
         uint32_t max_slices);

private:
    using buffer_t =
            std::array<char, net::socket_channel::buffer_size>;

    using bool_vec_t =
            std::vector<bool>;

    using merge_requests_t =
            std::queue<uint32_t>;

    using ushards_t =
            std::vector<ushard>;

private:
    std::string_view m_path;

    uint64_t m_next_tid{1};
    uint32_t m_max_slices{16384};

    uint32_t m_slice_count{0};
    gt::condition m_slice_count_cond;

    gt::condition m_merge_cond;
    bool_vec_t m_merge_locks;

    bool_vec_t m_merge_request_filter;
    merge_requests_t m_merge_requests;

    uint64_t m_merged_keys{0};
    uint64_t m_merged_size{0};

    ushards_t m_ushards;

private:
    uint32_t merge_id_from(uint16_t ushard_id, uint8_t tier_id);
    void request_merge_if_needed(uint16_t ushard_id, uint8_t tier_id);
    void merge(uint32_t merge_id);

    void merge_thread();

    void print_rate();
};

}
