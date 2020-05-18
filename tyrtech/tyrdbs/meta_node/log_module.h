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
        using ref_t =
                slab_list<uint32_t, 32768>;

        using refs_t =
                std::unordered_map<uint32_t, ref_t>;

    private:
        class impl* impl{nullptr};

        uint16_t merging_ushard_id{0};
        bool merge_in_progress{false};

        uint32_t next_handle{1};

        refs_t m_refs;

    private:
        context(class impl* impl);

    private:
        friend class impl;
    };

public:
    context create_context(const std::shared_ptr<io::socket>& remote);

public:
    void fetch_slices(const fetch_slices::request_parser_t& request,
                      net::socket_channel* channel,
                      context* ctx);

    void release_slices(const release_slices::request_parser_t& request,
                        net::socket_channel* channel,
                        context* ctx);

    void update_slices(const update_slices::request_parser_t& request,
                       net::socket_channel* channel,
                       context* ctx);

    void fetch_slices_to_merge(const fetch_slices_to_merge::request_parser_t& request,
                               net::socket_channel* channel,
                               context* ctx);

    void merge_slices(const merge_slices::request_parser_t& request,
                      net::socket_channel* channel,
                      context* ctx);

public:
    impl(const std::string_view& path,
         uint32_t max_slices,
         uint16_t ushards);

private:
    struct slice_entry
    {
        uint16_t ushard_id{0};
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

    using bool_vec_t =
            std::vector<bool>;

    using merge_requests_t =
            slab_list<uint16_t, 128>;

private:
    slices_t::entry_pool_t m_slice_pool;
    merge_requests_t::entry_pool_t m_merge_request_pool;
    context::ref_t::entry_pool_t m_ref_pool;

    uint64_t m_next_tid{1};

    uint32_t m_max_slices{0};
    uint32_t m_slice_count{0};
    gt::condition m_slice_count_cond;

    bool_vec_t m_merge_locks;
    bool_vec_t m_merges_requested;
    merge_requests_t m_merge_requests{&m_merge_request_pool};

    gt::condition m_merge_cond;

    ushards_t m_ushards;

    slices_t m_removed_slices{&m_slice_pool};

private:
    slices_t load_transaction(net::socket_channel* channel, int32_t ushard_id);
    uint64_t commit(slices_t* transaction);

    bool load_block(const block_parser& block, slices_t* transaction, int32_t ushard_id);

    uint16_t get_ushard_id_to_merge();
    void release_merge_for(uint16_t ushard_id);
    void signal_merge_if_needed(uint16_t ushard_id);

    void incref(const slices_t& slices, context::ref_t* ref);
    void decref(const context::ref_t& ref);

    void print_rate();
};

}
