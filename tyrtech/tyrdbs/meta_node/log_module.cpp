#include <common/logger.h>
#include <common/clock.h>
#include <common/rdrnd.h>
#include <gt/async.h>
#include <io/file.h>
#include <net/rpc_response.h>
#include <tyrdbs/writer.h>
#include <tyrdbs/meta_node/log_module.h>


namespace tyrtech::tyrdbs::meta_node::log {


class file_reader : public tyrdbs::reader
{
public:
    uint32_t pread(uint64_t offset, char* data, uint32_t size) const override
    {
        auto orig_size = size;

        while (size != 0)
        {
            auto res = m_file.pread(offset, data, size);

            if (res == 0)
            {
                throw io::file::exception("{}: unexpected end of file", m_file.path());
            }

            data += res;
            size -= res;

            offset += res;
        }

        return orig_size;
    }

    void unlink() override
    {
        m_file.unlink();
    }

public:
    template<typename... Arguments>
    file_reader(Arguments&&... arguments)
      : m_file(io::file::open(io::file::access::read_only, std::forward<Arguments>(arguments)...))
    {
    }

private:
    io::file m_file;
};


class file_writer : public tyrdbs::writer
{
public:
    uint32_t write(const char* data, uint32_t size) override
    {
        m_writer.write(data, size);
        return size;
    }

    void flush() override
    {
        m_writer.flush();
    }

    uint64_t offset() const override
    {
        return m_writer.offset();
    }

    void unlink() override
    {
        m_file.unlink();
    }

    std::string_view path() const
    {
        return m_file.path();
    }

public:
    template<typename... Arguments>
    file_writer(Arguments&&... arguments)
      : m_file(io::file::create(std::forward<Arguments>(arguments)...))
    {
    }

private:
    class stream_writer
    {
    public:
        uint32_t write(const char* data, uint32_t size)
        {
            auto orig_size = size;

            while (size != 0)
            {
                auto res = m_file->pwrite(m_offset, data, size);
                assert(res != 0);

                data += res;
                size -= res;

                m_offset += res;
            }

            return orig_size;
        }

        uint64_t offset() const
        {
            return m_offset;
        }

    public:
        stream_writer(io::file* file)
          : m_file(file)
        {
        }

    private:
        io::file* m_file{nullptr};
        uint64_t m_offset{0};
    };

private:
    using buffer_t =
            std::array<char, tyrdbs::node::page_size>;

    using writer_t =
            buffered_writer<buffer_t, stream_writer>;

private:
    io::file m_file;

    buffer_t m_stream_buffer;
    stream_writer m_stream_writer{&m_file};

    writer_t m_writer{&m_stream_buffer, &m_stream_writer};
};


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
    net::rpc_response<log::fetch> response(ctx->channel);
    auto message = response.add_message();

    response.send();

    suspend_writers();

    try
    {
        if (request.has_ushard_id() == true)
        {
            uint16_t ushard_id = request.ushard_id() % m_ushards.size();

            auto slices = m_ushards[ushard_id].get();

            if (request.has_min_key() == true && request.has_max_key() == true)
            {
                auto it = overwrite_iterator(request.min_key(), request.max_key(), std::move(slices));
                send_keys(ushard_id, &it, ctx->channel);
            }
            else
            {
                auto it = overwrite_iterator(std::move(slices));
                send_keys(ushard_id, &it, ctx->channel);
            }
        }
        else
        {
            for (uint16_t ushard_id = 0; ushard_id < m_ushards.size(); ushard_id++)
            {
                auto slices = m_ushards[ushard_id].get();

                if (request.has_min_key() == true && request.has_max_key() == true)
                {
                    auto it = overwrite_iterator(request.min_key(), request.max_key(), std::move(slices));
                    send_keys(ushard_id, &it, ctx->channel);
                }
                else
                {
                    auto it = overwrite_iterator(std::move(slices));
                    send_keys(ushard_id, &it, ctx->channel);
                }
            }
        }
    }
    catch (...)
    {
        resume_writers();

        throw;
    }

    transaction_log_t transaction_log;
    register_transaction_log(&transaction_log);

    resume_writers();

    try
    {
        m_transaction_log_condition.wait();
        assert(transaction_log.empty() == false);

        auto transaction = transaction_log.pop();

        auto tid = std::get<0>(transaction);
        auto& blocks = std::get<1>(transaction);

        net::rpc_response<log::fetch> response(ctx->channel);
        auto message = response.add_message();

        message.add_tid(tid);

        response.send();

        for (auto& block : *blocks)
        {
            ctx->channel->write(block.data(), block.size());
        }

        unregister_transaction_log();
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

    register_writer();

    try
    {
        auto tid = m_next_tid++;

        for (auto& it : std::get<0>(transaction))
        {
            auto ushard_id = it.first % m_ushards.size();
            auto& ushard = m_ushards[ushard_id];

            auto& slice = it.second;
            slice->set_tid(tid);

            auto tier_id = ushard::tier_id_of(slice->key_count());

            if (ushard.add(tier_id, std::move(slice)) == true)
            {
                request_merge_if_needed(ushard_id, tier_id);
            }

            m_slice_count++;
        }

        push_transaction(tid, std::move(std::get<1>(transaction)));

        unregister_writer();

        net::rpc_response<log::update> response(ctx->channel);
        auto message = response.add_message();

        message.set_tid(tid);

        response.send();
    }
    catch (...)
    {
        unregister_writer();

        throw;
    }
}

void impl::merge(uint32_t merge_id)
{
    auto ushard_id = merge_id / ushard::max_tiers;
    auto tier_id = merge_id % ushard::max_tiers;

    auto& ushard = m_ushards[ushard_id];

    if (ushard.needs_merge(tier_id) == false)
    {
        return;
    }

    auto slices = ushard.get(tier_id);

    auto fw = std::make_shared<file_writer>("data/{:016x}.dat", rdrnd());
    auto writer = std::make_unique<slice_writer>(fw);

    auto it = overwrite_iterator(slices);

    writer->add(&it, false);

    writer->flush();
    writer->commit();

    auto reader = std::make_shared<file_reader>(fw->path());
    auto slice = std::make_shared<tyrdbs::slice>(fw->offset(),
                                                 writer->cache_id(),
                                                 std::move(reader));

    m_merged_keys += slice->key_count();
    m_merged_size += fw->offset();

    if (ushard.remove(tier_id, slices.size()) == true)
    {
        request_merge_if_needed(ushard_id, tier_id);
    }

    auto new_tier_id = ushard::tier_id_of(slice->key_count());

    if (ushard.add(new_tier_id, std::move(slice)) == true)
    {
        request_merge_if_needed(ushard_id, new_tier_id);
    }

    assert(m_slice_count >= slices.size());
    m_slice_count -= slices.size();

    m_slice_count++;

    if (m_slice_count <= m_max_slices)
    {
        m_slice_count_cond.signal_all();
    }

    for (auto& s : slices)
    {
        s->unlink();
    }
}

void impl::merge_thread()
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

        auto merge_id = m_merge_requests.front();
        m_merge_requests.pop();

        assert(m_merge_request_filter[merge_id] == true);
        m_merge_request_filter[merge_id] = false;

        if (m_merge_locks[merge_id] == true)
        {
            continue;
        }

        m_merge_locks[merge_id] = true;

        merge(merge_id);

        m_merge_locks[merge_id] = false;

        gt::yield();
    }
}

void impl::request_merge_if_needed(uint16_t ushard_id, uint8_t tier_id)
{
    auto merge_id = merge_id_from(ushard_id, tier_id);

    if (m_merge_request_filter[merge_id] == true)
    {
        return;
    }

    m_merge_request_filter[merge_id] = true;
    m_merge_requests.push(merge_id);

    m_merge_cond.signal();
}

uint32_t impl::merge_id_from(uint16_t ushard_id, uint8_t tier_id)
{
    return (ushard_id * ushard::max_tiers) + tier_id;
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
            throw invalid_request_exception("merge results block too big");
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
            auto slice = std::make_shared<tyrdbs::slice>(fw->offset(),
                                                         writer->cache_id(),
                                                         std::move(reader));

            slices[ushard_id] = std::move(slice);
        };

        jobs.run(std::move(f));
    }

    jobs.wait();

    return transaction;
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

        logger::notice("{:.2f} transactions/s, merged: {} keys ({:.2f} MB)",
                       (cur_requests - last_requests) / total_t,
                       m_merged_keys - last_merged_keys,
                       (m_merged_size - last_merged_size) / (1024. * 1024));

        last_requests = cur_requests;
        last_timestamp = cur_timestamp;

        last_merged_keys = m_merged_keys;
        last_merged_size = m_merged_size;
    }
}

void impl::register_writer()
{
    while (m_suspend_count != 0)
    {
        m_suspend_condition.wait();
    }

    m_writer_count++;
}

void impl::unregister_writer()
{
    m_writer_count--;

    if (m_writer_count == 0 &&
        m_suspend_count != 0)
    {
        m_writer_condition.signal_all();
    }
}

void impl::suspend_writers()
{
    m_suspend_count++;

    while (m_writer_count != 0)
    {
        m_writer_condition.wait();
    }
}

void impl::resume_writers()
{
    m_suspend_count--;

    if (m_suspend_count == 0)
    {
        m_suspend_condition.signal_all();
    }
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

void impl::send_keys(uint16_t ushard_id, overwrite_iterator* it, net::socket_channel* channel)
{
    using buffer_t =
            std::array<char, net::socket_channel::buffer_size>;

    buffer_t buffer;

    auto builder = message::builder(buffer.data(), buffer.size());
    auto block = tyrdbs::meta_node::block_builder(&builder);

    auto entries = block.add_entries();

    while (it->next() == true)
    {
        uint32_t bytes_required = 0;

        bytes_required += tyrdbs::meta_node::block_builder::entries_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::key_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::value_bytes_required();
        bytes_required += tyrdbs::meta_node::entry_builder::tid_bytes_required();
        bytes_required += it->key().size();
        bytes_required += it->value().size();

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
        entry.set_ushard_id(ushard_id);
        entry.add_tid(it->tid());
        entry.add_key(it->key());
        entry.add_value(it->value());
    }

    block.set_flags(1);
    block.finalize();

    channel->write(buffer.data(), builder.size());
    channel->flush();
}

impl::impl(const std::string_view& path,
           uint32_t merge_threads,
           uint32_t ushards,
           uint32_t max_slices)
  : m_path(path)
  , m_max_slices(max_slices)
{
    for (uint32_t i = 0; i < merge_threads; i++)
    {
        gt::create_thread(&impl::merge_thread, this);
    }

    m_merge_locks.resize(ushards * ushard::max_tiers);
    m_merge_request_filter.resize(ushards * ushard::max_tiers);

    m_ushards.resize(ushards);

    gt::create_thread(&impl::print_rate, this);
}

}
