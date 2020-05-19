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
    while (m_slice_count > m_max_slices)
    {
        m_slice_count_cond.wait();
    }

    auto id = request.id();
    auto size = request.size();
    auto key_count = request.key_count();

    auto tid = m_next_tid++;

    auto tier = tier_of(key_count);

    m_tiers[tier].emplace_back(slice());
    auto& slice = m_tiers[tier].back();

    slice.id = id;
    slice.tid = tid;
    slice.size = size;
    slice.key_count = key_count;

    m_slice_count++;

    request_merge_if_needed(tier);

    net::rpc_response<log::update> response(channel);
    auto message = response.add_message();

    message.set_tid(tid);

    response.send();
}

void impl::merge(const merge::request_parser_t& request,
                 net::socket_channel* channel,
                 context* ctx)
{
    merge_thread(channel);

    net::rpc_response<log::merge> response(channel);
    auto message = response.add_message();

    message.add_terminate(1);

    response.send();
}

uint32_t impl::tier_of(uint64_t key_count)
{
    return ((64 - __builtin_clzll(key_count)) >> 2) - 1;
}

void impl::merge(net::socket_channel* channel, uint8_t tier)
{
    auto tier_size = m_tiers[tier].size();

    if (tier_size <= 4)
    {
        return;
    }

    // merge rpc

    net::rpc_response<log::merge> response(channel);
    auto message = response.add_message();

    response.send();

    //send slice list

    channel->flush();

    char c;
    channel->read(&c, 1);

    tier_size = m_tiers[tier].size();
    m_tiers[tier].clear();

    auto new_tier = std::min(tier + 1, 31);

    m_tiers[new_tier].emplace_back(slice());
    auto& slice = m_tiers[new_tier].back();

    slice.id = 1;
    slice.tid = 0;
    slice.size = 2;
    slice.key_count = 3;

    // merge rpc

    request_merge_if_needed(tier);
    request_merge_if_needed(new_tier);

    assert(m_slice_count >= tier_size);
    m_slice_count -= tier_size;

    m_slice_count++;

    if (m_slice_count <= m_max_slices)
    {
        m_slice_count_cond.signal_all();
    }
}

void impl::merge_thread(net::socket_channel* channel)
{
    while (true)
    {
        while (m_merge_requests.empty() == true)
        {
            m_merge_cond.wait();
        }

        if (gt::terminated() == true)
        {
            break;
        }

        auto tier = m_merge_requests.front();
        m_merge_requests.pop();

        assert(m_merge_request_filter[tier] == true);
        m_merge_request_filter[tier] = false;

        // if (m_merge_locks[tier] == true)
        // {
        //     m_merge_request_filter[tier] = true;
        //     m_merge_requests.push(tier);

        //     m_merge_cond.signal();

        //     continue;
        // }

        m_merge_locks[tier] = true;

        merge(channel, tier);

        m_merge_locks[tier] = false;
    }

    net::rpc_response<log::merge> response(channel);
    auto message = response.add_message();

    message.add_terminate(1);

    response.send();
}

void impl::request_merge_if_needed(uint8_t tier)
{
    if (m_tiers[tier].size() <= 4)
    {
        return;
    }

    if (m_merge_request_filter[tier] == true)
    {
        return;
    }

    m_merge_request_filter[tier] = true;
    m_merge_requests.push(tier);

    m_merge_cond.signal();
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

        logger::notice("{:.2f} transactions/s",
                       1000000000. * (cur_requests - last_requests) / (cur_timestamp - last_timestamp));

        last_requests = cur_requests;
        last_timestamp = cur_timestamp;
    }
}

impl::impl(const std::string_view& path, uint32_t max_slices)
  : m_max_slices(max_slices)
{
    gt::create_thread(&impl::print_rate, this);

    m_merge_locks.resize(max_tiers);
    m_merge_request_filter.resize(max_tiers);
    m_tiers.resize(max_tiers);
}

}
