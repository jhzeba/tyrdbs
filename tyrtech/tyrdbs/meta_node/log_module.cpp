#include <common/uuid.h>
#include <common/logger.h>
#include <common/clock.h>
#include <gt/async.h>
#include <net/rpc_response.h>
#include <tyrdbs/overwrite_iterator.h>
#include <tyrdbs/meta_node/log_module.h>


namespace tyrtech::tyrdbs::meta_node::log {


impl::context::context(class impl* impl)
  : impl(impl)
{
}

impl::context::~context()
{
    if (merge_in_progress == true)
    {
        merge_in_progress = false;

        impl->release_merge_for(merging_ushard_id);
    }

    for (auto& it : m_refs)
    {
        impl->decref(it.second);
    }
}

impl::context::context(context&& other) noexcept
{
    *this = std::move(other);
}

impl::context& impl::context::operator=(impl::context&& other) noexcept
{
    merging_ushard_id = other.merging_ushard_id;
    merge_in_progress = other.merge_in_progress;

    other.merge_in_progress = false;

    return *this;
}

impl::context impl::create_context(const std::shared_ptr<io::socket>& remote)
{
    return context(this);
}

void impl::fetch_slices(const fetch_slices::request_parser_t& request,
                        net::socket_channel* channel,
                        context* ctx)
{
    if (unlikely(ctx->merge_in_progress == true))
    {
        throw invalid_request_exception("merge in progress");
    }

    auto& ushard = m_ushards[request.ushard_id() % m_ushards.size()];

    auto handle = ctx->next_handle++;
    auto it = ctx->m_refs.insert(context::refs_t::value_type(handle, context::ref_t(&m_ref_pool)));

    incref(ushard, &it.first->second);

    net::rpc_response<log::fetch_slices> response(channel);
    auto message = response.add_message();

    message.set_handle(handle);

    response.send();

    std::array<char, 8192> buffer;
    message::builder builder(buffer.data(), buffer.size());
    tyrdbs::meta_node::block_builder block(&builder);

    auto slices = block.add_slices();

    auto e = it.first->second.begin();

    while (e != context::ref_t::invalid_handle)
    {
        auto entry = m_ushards[0].item(*it.first->second.item(e));

        uint16_t bytes_required = 0;

        bytes_required += tyrdbs::meta_node::slice_builder::id_bytes_required();
        bytes_required += tyrdbs::meta_node::slice_builder::tid_bytes_required();
        bytes_required += sizeof(uuid_t);

        if (builder.available_space() <= bytes_required)
        {
            slices.finalize();
            block.finalize();

            channel->write(buffer.data(), builder.size());

            builder = message::builder(buffer.data(), buffer.size());
            block = tyrdbs::meta_node::block_builder(&builder);

            slices = block.add_slices();
        }

        auto slice = slices.add_value();

        slice.set_ushard_id(entry->ushard_id);
        slice.set_size(entry->size);
        slice.add_tid(entry->tid);
        slice.add_id(std::string_view(reinterpret_cast<const char*>(entry->id), sizeof(uuid_t)));

        e = it.first->second.next(e);
    }

    block.set_flags(1);

    slices.finalize();
    block.finalize();

    channel->write(buffer.data(), builder.size());
}

void impl::release_slices(const release_slices::request_parser_t& request,
                          net::socket_channel* channel,
                          context* ctx)
{
    if (unlikely(ctx->merge_in_progress == true))
    {
        throw invalid_request_exception("merge in progress");
    }

    auto handle = request.handle();
    auto it = ctx->m_refs.find(handle);

    if (it == ctx->m_refs.end())
    {
        throw invalid_request_exception("invalid handle specified");
    }

    decref(it->second);
    ctx->m_refs.erase(it);

    net::rpc_response<log::release_slices> response(channel);
    auto message = response.add_message();

    response.send();
}

void impl::update_slices(const update_slices::request_parser_t& request,
                         net::socket_channel* channel,
                         context* ctx)
{
    if (unlikely(ctx->merge_in_progress == true))
    {
        throw invalid_request_exception("merge in progress");
    }

    auto transaction = load_transaction(channel, -1);

    while (m_slice_count > m_max_slices)
    {
        m_slice_count_cond.wait();
    }

    auto tid = commit(&transaction);

    net::rpc_response<log::update_slices> response(channel);
    auto message = response.add_message();

    message.set_tid(tid);

    response.send();
}

void impl::fetch_slices_to_merge(const fetch_slices_to_merge::request_parser_t& request,
                                 net::socket_channel* channel,
                                 context* ctx)
{
    if (unlikely(ctx->merge_in_progress == true))
    {
        throw invalid_request_exception("merge in progress");
    }

    auto ushard_id = get_ushard_id_to_merge();

    ctx->merge_in_progress = true;
    ctx->merging_ushard_id = ushard_id;

    net::rpc_response<log::fetch_slices_to_merge> response(channel);
    auto message = response.add_message();

    message.set_ushard_id(ushard_id);

    response.send();

    // send_slices
}

void impl::merge_slices(const merge_slices::request_parser_t& request,
                        net::socket_channel* channel,
                        context* ctx)
{
    if (unlikely(ctx->merge_in_progress == false))
    {
        throw invalid_request_exception("merge not in progress");
    }

    auto ushard_id = request.ushard_id();

    if (unlikely(ctx->merging_ushard_id != ushard_id))
    {
        throw invalid_request_exception("merge for wrong ushard_id requested");
    }

    auto transaction = load_transaction(channel, ushard_id);
    commit(&transaction);

    release_merge_for(ushard_id);

    ctx->merge_in_progress = false;

    net::rpc_response<log::update_slices> response(channel);
    auto message = response.add_message();

    response.send();
}

impl::slices_t impl::load_transaction(net::socket_channel* channel, int32_t ushard_id)
{
    slices_t transaction(&m_slice_pool);

    uint64_t total_size = 0;
    bool is_last_block = false;

    while (is_last_block == false)
    {
        buffer_t block_buffer;
        uint16_t* block_size = reinterpret_cast<uint16_t*>(block_buffer.data());

        channel->read(block_size);
        total_size += *block_size;

        if (unlikely(*block_size > net::socket_channel::buffer_size - sizeof(uint16_t)))
        {
            throw invalid_request_exception("block too big");
        }

        channel->read(block_buffer.data() + sizeof(uint16_t), *block_size);

        message::parser parser(block_buffer.data(),
                               *block_size + sizeof(uint16_t));

        is_last_block = load_block(block_parser(&parser, 0), &transaction, ushard_id);
    }

    static bool print_done = false;

    if (print_done == false)
    {
        logger::notice("total size: {}", total_size);
        print_done = true;
    }

    return transaction;
}

bool impl::load_block(const block_parser& block, slices_t* transaction, int32_t ushard_id)
{
    bool is_last_block = (block.flags() & 0x01) == 1; // TODO: add block flags

    if (unlikely(block.has_slices() == false))
    {
        return is_last_block;
    }

    auto slices = block.slices();

    while (slices.next() == true)
    {
        auto slice = slices.value();

        if (slice.has_id() == false)
        {
            continue;
        }

        auto id = slice.id();

        if (unlikely(id.size() != sizeof(uuid_t)))
        {
            throw invalid_request_exception("invalid slice id");
        }

        if (ushard_id != -1)
        {
            if (slice.ushard_id() != ushard_id)
            {
                throw invalid_request_exception("invalid ushard_id specified");
            }
        }

        auto entry = transaction->item(transaction->push_back());

        entry->ushard_id = slice.ushard_id() % m_ushards.size();
        entry->ref = 0;
        entry->tid = 0;
        entry->size = slice.size();
        std::memcpy(entry->id, id.data(), sizeof(uuid_t));

        if ((slice.flags() & 0x01) == 0) // TODO: add slice flags
        {
            entry->ref |= 0x8000;
        }
    }

    return is_last_block;
}

uint64_t impl::commit(slices_t* transaction)
{
    auto tid = m_next_tid++;
    auto e = transaction->begin();

    while (e != slices_t::invalid_handle)
    {
        auto next_e = transaction->next(e);
        auto entry = transaction->item(e);

        entry->tid = tid;

        auto& ushard = m_ushards[entry->ushard_id];

        if ((entry->ref & 0x8000) != 0)
        {
            auto ushard_e = ushard.begin();

            while (ushard_e != slices_t::invalid_handle)
            {
                auto next_ushard_e = ushard.next(ushard_e);
                auto ushard_entry = ushard.item(ushard_e);

                if (std::memcmp(entry->id, ushard_entry->id, sizeof(uuid_t)) == 0)
                {
                    break;
                }

                ushard_e = next_ushard_e;
            }

            m_removed_slices.move_back(&ushard, ushard_e);

            m_slice_count--;
        }
        else
        {
            ushard.move_back(transaction, e);

            m_slice_count++;
        }

        signal_merge_if_needed(entry->ushard_id);

        e = next_e;
    }

    return tid;
}

uint16_t impl::get_ushard_id_to_merge()
{
    uint16_t ushard_id;

    while (true)
    {
        m_merge_cond.wait();

        ushard_id = m_merge_requests.front();
        m_merge_requests.pop_front();

        m_merges_requested[ushard_id] = false;

        bool merge_cond = true;

        merge_cond &= m_ushards[ushard_id].size() > 16;
        merge_cond &= m_merge_locks[ushard_id] == false;

        if (merge_cond == true)
        {
            break;
        }
    }

    m_merge_locks[ushard_id] = true;

    return ushard_id;
}

void impl::release_merge_for(uint16_t ushard_id)
{
    assert(likely(m_merge_locks[ushard_id] == true));
    m_merge_locks[ushard_id] = false;
}

void impl::signal_merge_if_needed(uint16_t ushard_id)
{
    if (m_ushards[ushard_id].size() < 16)
    {
        return;
    }

    if (m_merges_requested[ushard_id] == true)
    {
        return;
    }

    m_merges_requested[ushard_id] = true;
    m_merge_requests.push_back(ushard_id);

    m_merge_cond.signal();
}

void impl::incref(const slices_t& slices, context::ref_t* ref)
{
    auto e = slices.begin();

    while (e != slices_t::invalid_handle)
    {
        auto entry = slices.item(e);
        entry->ref++;

        ref->push_back(e);

        e = slices.next(e);
    }
}

void impl::decref(const context::ref_t& ref)
{
    auto e = ref.begin();

    while (e != context::ref_t::invalid_handle)
    {
        auto entry = m_ushards[0].item(*ref.item(e));
        assert(likely((entry->ref & 0x7fff) != 0));

        entry->ref--;

        e = ref.next(e);
    }
}

void impl::print_rate()
{
    uint64_t last_requests = m_next_tid;
    uint64_t last_timestamp = clock::now();

    while (true)
    {
        gt::sleep(1000);

        uint64_t cur_requests = m_next_tid;
        uint64_t cur_timestamp = clock::now();

        logger::notice("requests: {:.2f} req/s",
                       1000000000. * (cur_requests - last_requests) / (cur_timestamp - last_timestamp));

        last_requests = cur_requests;
        last_timestamp = cur_timestamp;
    }
}

impl::impl(const std::string_view& path,
           uint32_t max_slices,
           uint16_t ushards)
  : m_max_slices(max_slices)
{
    gt::create_thread(&impl::print_rate, this);

    for (uint16_t i = 0; i < ushards; i++)
    {
        m_ushards.emplace_back(slices_t(&m_slice_pool));
    }

    m_merge_locks.resize(ushards);
    m_merges_requested.resize(ushards);
}

}
