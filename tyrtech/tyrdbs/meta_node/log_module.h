#pragma once


#include <gt/condition.h>
#include <tyrdbs/meta_node/modules.json.h>

#include <queue>


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
        context(impl* impl);

    private:
        friend class impl;
    };

public:
    context create_context(const std::shared_ptr<io::socket>& remote);

public:
    void fetch(const fetch::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx);

    void update(const update::request_parser_t& request,
                net::socket_channel* channel,
                context* ctx);

    void merge(const merge::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx);

public:
    impl(const std::string_view& path, uint32_t max_slices);

private:
    static constexpr uint32_t max_tiers{32};

private:
    struct slice
    {
        uint64_t id;
        uint64_t tid;
        uint64_t size;
        uint64_t key_count;
    } __attribute__ ((packed));

private:
    using buffer_t =
            std::array<char, net::socket_channel::buffer_size>;

    using tier_t =
            std::vector<slice>;

    using tiers_t =
            std::vector<tier_t>;

    using bool_vec_t =
            std::vector<bool>;

    using merge_requests_t =
            std::queue<uint8_t>;

private:
    uint64_t m_next_tid{1};
    uint32_t m_max_slices{0};

    uint32_t m_slice_count{0};
    gt::condition m_slice_count_cond;

    gt::condition m_merge_cond;
    bool_vec_t m_merge_locks;

    bool_vec_t m_merge_request_filter;
    merge_requests_t m_merge_requests;

    tiers_t m_tiers;

private:
    uint32_t tier_of(uint64_t key_count);

    void run_merge_loop(net::socket_channel* channel);
    void merge(net::socket_channel* channel, uint8_t tier);
    void request_merge_if_needed(uint8_t tier);

    void print_rate();
};

}
