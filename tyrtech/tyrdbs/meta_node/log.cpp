#include <common/logger.h>
#include <common/clock.h>
#include <common/rdrnd.h>
#include <gt/async.h>
#include <io/file.h>
#include <net/rpc_response.h>
#include <tyrdbs/writer.h>
#include <tyrdbs/overwrite_iterator.h>
#include <tyrdbs/meta_node/log.h>
#include <tyrdbs/meta_node/file_reader.h>
#include <tyrdbs/meta_node/file_writer.h>


namespace tyrtech::tyrdbs::meta_node::log {


impl::context::context(net::socket_channel* channel)
  : channel(channel)
{
}

impl::context impl::create_context(net::socket_channel* channel)
{
    return context(channel);
}

void impl::fetch(const fetch::request_parser_t& request, context* ctx)
{
    for (uint16_t ushard_id = 0; ushard_id < m_ushards.size(); ushard_id++)
    {
        net::rpc_response<log::fetch> response(ctx->channel);
        auto message = response.add_message();

        message.set_last_segment(ushard_id == m_ushards.size() - 1);

        response.send();

        send_keys(ushard_id, ctx->channel);
    }

    ctx->channel->flush();
}

void impl::watch(const watch::request_parser_t& request, context* ctx)
{
    transaction_log_t transaction_log;
    register_transaction_log(&transaction_log);

    try
    {
        while (true)
        {
            while (transaction_log.empty() == true)
            {
                m_transaction_log_condition.wait();
            }

            auto transaction = transaction_log.front();
            transaction_log.pop();

            auto tid = std::get<0>(transaction);
            auto& blocks = std::get<1>(transaction);

            net::rpc_response<log::watch> response(ctx->channel);
            auto message = response.add_message();

            message.set_tid(tid);

            response.send();

            for (auto& block : *blocks)
            {
                ctx->channel->write(block.data(), block.size());
            }

            ctx->channel->flush();
        }
    }
    catch (...)
    {
        unregister_transaction_log();

        throw;
    }
}

void impl::update(const update::request_parser_t& request, context* ctx)
{
    while (m_slice_count > m_max_slices)
    {
        m_slice_count_cond.wait();
    }

    auto transaction = process_transaction(ctx->channel);
    auto tid = m_next_tid++;

    for (auto& it : std::get<0>(transaction))
    {
        auto ushard_id = it.first % m_ushards.size();
        auto& ushard = m_ushards[ushard_id];

        auto& slice = it.second;

        slice->set_cache_id(m_next_cache_id++);
        slice->set_tid(tid);

        ushard.add(std::move(slice));

        m_slice_count++;
    }

    push_transaction(tid, std::move(std::get<1>(transaction)));

    net::rpc_response<log::update> response(ctx->channel);
    auto message = response.add_message();

    message.set_tid(tid);

    response.send();
}

bool impl::load_block(const block_parser& block, writers_t* writers)
{
    bool is_last_block = (block.flags() & 0x01) != 0;

    if (block.has_entries() == false)
    {
        return is_last_block;
    }

    auto entries = block.entries();

    while (entries.next() == true)
    {
        auto entry = entries.value();

        if (entry.has_key() == false)
        {
            continue;
        }

        std::string_view key;
        std::string_view value;

        key = entry.key();

        if (entry.has_value() == true)
        {
            value = entry.value();
        }

        if (entry.has_ushard_id() == false)
        {
            throw invalid_request_exception("ushard_id missing");
        }

        auto ushard_id = entry.ushard_id();
        auto flags = entry.flags();

        auto& writer = (*writers)[ushard_id];

        if (writer == nullptr)
        {
            auto fw = std::make_shared<file_writer>("{}/{:016x}.dat", m_path, rdrnd());
            writer = std::make_unique<slice_writer>(std::move(fw));
        }

        bool eor = (flags & 0x01) != 0;
        bool deleted = (flags & 0x02) != 0;

        writer->add(key, value, eor, deleted);
    }

    return is_last_block;
}

impl::transaction_t impl::process_transaction(net::socket_channel* channel)
{
    transaction_t transaction;

    auto& slices = std::get<0>(transaction);
    auto& blocks = std::get<1>(transaction);

    blocks = std::make_shared<blocks_t>();

    writers_t writers;

    bool is_last_block = false;

    while (is_last_block == false)
    {
        using buffer_t =
                std::array<char, net::socket_channel::buffer_size>;

        buffer_t buffer;
        uint16_t* size = reinterpret_cast<uint16_t*>(buffer.data());

        channel->read(size);

        if (unlikely(*size > net::socket_channel::buffer_size - sizeof(uint16_t)))
        {
            throw invalid_request_exception("block too big");
        }

        channel->read(buffer.data() + sizeof(uint16_t), *size);

        auto db = dynamic_buffer(*size + sizeof(uint16_t));
        memcpy(db.data(), buffer.data(), db.size());

        blocks->emplace_back(std::move(db));

        message::parser parser(buffer.data(), *size + sizeof(uint16_t));
        is_last_block = load_block(block_parser(&parser, 0), &writers);
    }

    auto jobs = gt::async::create_jobs();

    for (auto& it : writers)
    {
        auto f = [&slices,
                  &writer = it.second,
                  ushard_id = it.first]
        {
            writer->flush();
            writer->commit();

            file_writer* fw = static_cast<file_writer*>(writer->writer());

            auto reader = std::make_shared<file_reader>(fw->path());
            auto slice = std::make_shared<tyrdbs::slice>(fw->offset(), std::move(reader));

            slices[ushard_id] = std::move(slice);
        };

        jobs.run(std::move(f));
    }

    jobs.wait();

    return transaction;
}

void impl::register_transaction_log(transaction_log_t* transaction_log)
{
    auto it = m_transaction_log_map.find(gt::current_context());
    assert(it == m_transaction_log_map.end());

    m_transaction_log_map[gt::current_context()] = transaction_log;
}

void impl::unregister_transaction_log()
{
    auto it = m_transaction_log_map.find(gt::current_context());
    assert(it != m_transaction_log_map.end());

    m_transaction_log_map.erase(it);
}

void impl::push_transaction(uint64_t tid, blocks_ptr blocks)
{
    for (auto& it : m_transaction_log_map)
    {
        auto& transaction_log = it.second;
        transaction_log->push(std::make_tuple(tid, blocks));
    }

    m_transaction_log_condition.signal_all();
}

void impl::send_keys(uint16_t ushard_id, net::socket_channel* channel)
{
    using buffer_t =
            std::array<char, net::socket_channel::buffer_size>;

    buffer_t buffer;

    auto builder = message::builder(buffer.data(), buffer.size());
    auto block = tyrdbs::meta_node::block_builder(&builder);

    auto entries = block.add_entries();

    auto it = m_ushards[ushard_id].begin();

    while (it.next() == true)
    {
        if (it.deleted() == true)
        {
            continue;
        }

        uint32_t bytes_required = 0;

        bytes_required += tyrdbs::meta_node::block_builder::entries_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::key_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::value_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::tid_bytes_required();
        bytes_required += it.key().size();
        bytes_required += it.value().size();

        if (bytes_required > builder.available_space())
        {
            block.finalize();

            channel->write(buffer.data(), builder.size());

            builder = message::builder(buffer.data(), buffer.size());
            block = tyrdbs::meta_node::block_builder(&builder);

            entries = block.add_entries();
        }

        auto entry = entries.add_value();

        entry.set_flags(0x01);
        entry.add_tid(it.tid());
        entry.add_key(it.key());
        entry.add_value(it.value());
    }

    block.set_flags(1);
    block.finalize();

    channel->write(buffer.data(), builder.size());
}

impl::impl(const std::string_view& path,
           uint32_t merge_threads,
           uint32_t ushards,
           uint32_t max_slices)
  : m_path{path}
  , m_max_slices{max_slices}
{
    for (uint32_t i = 0; i < ushards; i++)
    {
        m_ushards.emplace_back(ushard{path});
    }
}

}
