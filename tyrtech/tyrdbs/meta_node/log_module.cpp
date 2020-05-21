#include <common/logger.h>
#include <common/clock.h>
#include <net/rpc_response.h>
#include <tyrdbs/meta_node/log_module.h>


namespace tyrtech::tyrdbs::meta_node::log {


impl::context::context(impl* impl)
{
}

impl::context::~context()
{
}

impl::context::context(context&& other) noexcept
{
    *this = std::move(other);
}

impl::context& impl::context::operator=(impl::context&& other) noexcept
{
    return *this;
}

impl::context impl::create_context(const std::shared_ptr<io::socket>& remote)
{
    return context(this);
}

void impl::fetch(const fetch::request_parser_t& request,
                 net::socket_channel* channel,
                 context* ctx)
{
    net::rpc_response<log::fetch> response(channel);
    auto message = response.add_message();

    response.send();

    // send keys
}

void impl::update(const update::request_parser_t& request,
                  net::socket_channel* channel,
                  context* ctx)
{
    while (m_slice_count > max_slices)
    {
        m_slice_count_cond.wait();
    }

    auto id = request.id();
    auto size = request.size();
    auto key_count = request.key_count();

    auto tid = m_next_tid++;

    auto tier_id = tier_id_of(key_count);

    m_tiers[tier_id].emplace_back(slice());
    auto& slice = m_tiers[tier_id].back();

    slice.id = id;
    slice.tid = tid;
    slice.size = size;
    slice.key_count = key_count;

    m_slice_count++;

    request_merge_if_needed(tier_id);

    net::rpc_response<log::update> response(channel);
    auto message = response.add_message();

    message.set_tid(tid);

    response.send();
}

void impl::merge(const merge::request_parser_t& request,
                 net::socket_channel* channel,
                 context* ctx)
{
    while (true)
    {
        while (m_merge_requests.empty() == true)
        {
            m_merge_cond.wait();

            if (gt::terminated() == true)
            {
                break;
            }
        }

        if (gt::terminated() == true)
        {
            break;
        }

        auto tier_id = m_merge_requests.front();
        m_merge_requests.pop();

        assert(m_merge_request_filter[tier_id] == true);
        m_merge_request_filter[tier_id] = false;

        if (m_merge_locks[tier_id] == true)
        {
            continue;
        }

        m_merge_locks[tier_id] = true;

        try
        {
            merge(channel, tier_id);
        }
        catch (...)
        {
            m_merge_locks[tier_id] = false;

            throw;
        }

        m_merge_locks[tier_id] = false;

        gt::yield();
    }

    net::rpc_response<log::merge> response(channel);
    auto message = response.add_message();

    message.add_terminate(1);

    response.send();
}

uint8_t impl::tier_id_of(uint64_t key_count)
{
    // static uint8_t i{0};
    // return (i++) % max_tiers;
    return ((64 - __builtin_clzll(key_count)) >> 2);
}

void impl::merge(net::socket_channel* channel, uint8_t tier_id)
{
    auto tier = m_tiers[tier_id];

    if (tier.size() <= 4)
    {
        return;
    }

    auto t1 = clock::now();

    {
        net::rpc_response<log::merge> response(channel);
        auto message = response.add_message();

        response.send();
    }

    send_tier(tier, true, channel);

    channel->flush();

    tier_t merged_tier = recv_tier(channel);

    if (merged_tier.size() != 1)
    {
        throw invalid_request_exception("too many merge results");
    }

    m_tiers[tier_id].erase(m_tiers[tier_id].begin(),
                           m_tiers[tier_id].begin() + tier.size());

    auto& merged_slice = merged_tier[0];
    auto merged_slice_tier_id = tier_id_of(merged_slice.key_count);

    m_tiers[merged_slice_tier_id].emplace_back(merged_slice);

    m_merged_keys += merged_slice.key_count;
    m_merged_size += merged_slice.size;

    request_merge_if_needed(tier_id);
    request_merge_if_needed(merged_slice_tier_id);

    assert(m_slice_count >= tier.size());
    m_slice_count -= tier.size();

    m_slice_count++;

    if (m_slice_count <= max_slices)
    {
        m_slice_count_cond.signal_all();
    }

    auto t2 = clock::now();

    fmt::print("merge took {:.2f} ms\n", (t2 - t1) / 1000000.);
}

void impl::request_merge_if_needed(uint8_t tier_id)
{
    if (m_tiers[tier_id].size() <= 4)
    {
        return;
    }

    if (m_merge_request_filter[tier_id] == true)
    {
        return;
    }

    m_merge_request_filter[tier_id] = true;
    m_merge_requests.push(tier_id);

    m_merge_cond.signal();
}

impl::tier_t impl::recv_tier(net::socket_channel* channel)
{
    tier_t tier;

    bool is_last_block = false;

    while (is_last_block == false)
    {
        buffer_t buffer;
        uint16_t* size = reinterpret_cast<uint16_t*>(buffer.data());

        channel->read(size);

        if (unlikely(*size > net::socket_channel::buffer_size - sizeof(uint16_t)))
        {
            throw invalid_request_exception("merge results block too big");
        }

        channel->read(buffer.data() + sizeof(uint16_t), *size);

        message::parser parser(buffer.data(), *size + sizeof(uint16_t));
        is_last_block = load_block(block_parser(&parser, 0), &tier);
    }

    return tier;
}

bool impl::load_block(const block_parser& block, tier_t* tier)
{
    bool is_last_block = (block.flags() & 0x01) != 0;

    if (block.has_slices() == false)
    {
        return is_last_block;
    }

    auto slices = block.slices();

    while (slices.next() == true)
    {
        auto slice = slices.value();

        tier->emplace_back(impl::slice());
        auto& tier_slice = tier->back();

        tier_slice.id = slice.id();
        tier_slice.tid = slice.tid();
        tier_slice.size = slice.size();
        tier_slice.key_count = slice.key_count();
    }

    return is_last_block;
}

void impl::send_tier(const tier_t& tier, bool signal_last_block, net::socket_channel* channel)
{
    buffer_t buffer;

    auto builder = message::builder(buffer.data(), buffer.size());
    auto block = block_builder(&builder);
    auto slices = block.add_slices();

    for (auto& tier_slice : tier)
    {
        uint16_t bytes_required = 0;

        bytes_required += block_builder::slices_bytes_required();
        bytes_required += slice_builder::bytes_required();

        if (bytes_required > builder.available_space())
        {
            slices.finalize();
            block.finalize();

            channel->write(buffer.data(), builder.size());

            builder = message::builder(buffer.data(), buffer.size());
            block = block_builder(&builder);
            slices = block.add_slices();
        }

        auto slice = slices.add_value();

        slice.set_id(tier_slice.id);
        slice.set_size(tier_slice.size);
        slice.set_tid(tier_slice.tid);
        slice.set_key_count(tier_slice.key_count);
    }

    if (signal_last_block == true)
    {
        block.set_flags(1);
    }

    slices.finalize();
    block.finalize();

    channel->write(buffer.data(), builder.size());
}

void impl::print_rate()
{
    uint64_t last_requests = m_next_tid;
    uint64_t last_timestamp = clock::now();

    uint64_t last_merged_keys = m_merged_keys;
    uint64_t last_merged_size = m_merged_size;

    while (true)
    {
        gt::sleep(1000);

        uint64_t cur_requests = m_next_tid;
        uint64_t cur_timestamp = clock::now();

        float total_t = (cur_timestamp - last_timestamp) / 1000000000.;

        logger::notice("{:.2f} transactions/s, merge {:.2f} keys/s / {:.2f} MB/s",
                       (cur_requests - last_requests) / total_t,
                       (m_merged_keys - last_merged_keys) / total_t,
                       (m_merged_size - last_merged_size) / (1024 * 1024 * total_t));

        last_requests = cur_requests;
        last_timestamp = cur_timestamp;

        last_merged_keys = m_merged_keys;
        last_merged_size = m_merged_size;
    }
}

impl::impl(const std::string_view& path)
{
    gt::create_thread(&impl::print_rate, this);

    m_merge_locks.resize(max_tiers);
    m_merge_request_filter.resize(max_tiers);
    m_tiers.resize(max_tiers);
}

}
