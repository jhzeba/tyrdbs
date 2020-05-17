#include <common/uuid.h>
#include <common/logger.h>
#include <common/clock.h>
#include <gt/async.h>
#include <net/rpc_response.h>
#include <tyrdbs/overwrite_iterator.h>
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

void impl::update_slices(const update_slices::request_parser_t& request,
                         net::socket_channel* channel,
                         context* ctx)
{
    auto tid = update_slices(channel);

    net::rpc_response<log::update_slices> response(channel);
    auto message = response.add_message();

    message.set_tid(tid);

    response.send();
}

void impl::fetch_slices(const fetch_slices::request_parser_t& request,
                        net::socket_channel* channel,
                        context* ctx)
{
    net::rpc_response<log::fetch_slices> response(channel);
    auto message = response.add_message();

    response.send();
}

void impl::get_merge_candidates(const get_merge_candidates::request_parser_t& request,
                                net::socket_channel* channel,
                                context* ctx)
{
    net::rpc_response<log::get_merge_candidates> response(channel);
    auto message = response.add_message();

    response.send();
}

uint64_t impl::update_slices(net::socket_channel* channel)
{
    slices_t transaction(&m_slices);

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
            throw block_too_big_exception("block too big");
        }

        channel->read(block_buffer.data() + sizeof(uint16_t), *block_size);

        message::parser parser(block_buffer.data(),
                               *block_size + sizeof(uint16_t));

        is_last_block = process_block(block_parser(&parser, 0), &transaction);
    }

    static bool print_done = false;

    if (print_done == false)
    {
        logger::notice("total size: {}", total_size);
        print_done = true;
    }

    auto e = transaction.begin();

    while (e != slices_t::invalid_handle)
    {
        auto next_e = transaction.next(e);
        auto entry = transaction.item(e);

        auto& ushard = m_ushards[entry->ushard];

        if ((entry->ref & 0x8000) != 0)
        {
            // auto ushard_e = ushard.begin();

            // while (ushard_e != slices_t::invalid_handle)
            // {
            //     auto next_ushard_e = ushard.next(ushard_e);
            //     auto ushard_entry = ushard.item(ushard_e);

            //     if (std::memcmp(entry->id, ushard_entry->id, sizeof(uuid_t)) == 0)
            //     {
            //         break;
            //     }

            //     ushard_e = next_ushard_e;
            // }

            // m_removed_slices.move_back(ushard_e);
        }
        else
        {
            // ushard.move_back(e);
        }

        e = next_e;
    }

    return m_next_tid++;
}

bool impl::process_block(const block_parser& block, slices_t* transaction)
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
        auto flags = slice.flags();
        auto ushard = slice.ushard();

        if (slice.has_id() == false)
        {
            continue;
        }

        auto id = slice.id();

        if (unlikely(id.size() != sizeof(uuid_t)))
        {
            throw invalid_id_exception("invalid slice id");
        }

        auto entry = transaction->item(transaction->push_back());

        entry->ushard = ushard % m_ushards.size();
        entry->ref = 0;
        entry->tid = 0;
        std::memcpy(entry->id, id.data(), sizeof(uuid_t));

        if ((flags & 0x01) == 0) // TODO: add slice flags
        {
            entry->ref |= 0x8000;
        }
    }

    return is_last_block;
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
        m_ushards.emplace_back(slices_t(&m_slices));
    }
}

}
