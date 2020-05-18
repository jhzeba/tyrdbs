#pragma once


#include <common/uuid.h>
#include <common/slab_list.h>
#include <gt/condition.h>
#include <tyrdbs/slice_writer.h>
#include <tyrdbs/iterator.h>
#include <tyrdbs/meta_node/modules.json.h>
#include <tyrdbs/meta_node/block.json.h>


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
    void update_slices(const update_slices::request_parser_t& request,
                       net::socket_channel* channel,
                       context* ctx);

    void fetch_slices(const fetch_slices::request_parser_t& request,
                      net::socket_channel* channel,
                      context* ctx);

    void get_merge_candidates(const get_merge_candidates::request_parser_t& request,
                              net::socket_channel* channel,
                              context* ctx);

public:
    impl(const std::string_view& path,
         uint32_t max_slices,
         uint16_t ushards);

private:
    struct slice_entry
    {
        uint16_t ushard{0};
        uint16_t ref{0};
        uint64_t tid{0};
        uint64_t size{0};
        uuid_t id;
    } __attribute__ ((packed));

private:
    using buffer_t =
            std::array<char, net::socket_channel::buffer_size>;

    using slices_t =
            slab_list<slice_entry, 32768>;

    using ushards_t =
            std::vector<slices_t>;

    using merge_locks_t =
            std::vector<bool>;

    using merge_requests_t =
            slab_list<uint16_t, 128>;

private:
    merge_requests_t::entry_pool_t m_merge;
    slices_t::entry_pool_t m_slices;

    uint64_t m_next_tid{1};

    uint32_t m_max_slices{0};
    uint32_t m_slice_count{0};
    gt::condition m_slice_count_cond;

    merge_locks_t m_merge_locks;
    gt::condition m_merge_cond;

    ushards_t m_ushards;

    slices_t m_removed_slices{&m_slices};

private:
    uint64_t update_slices(net::socket_channel* channel, bool merge_request);

    bool process_block(const block_parser& block, slices_t* transaction);
    uint64_t commit(slices_t* transaction);
    void signal_merge_if_needed(uint16_t ushard);

    void print_rate();
};

}
