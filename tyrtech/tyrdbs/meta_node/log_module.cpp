#include <common/logger.h>
#include <common/clock.h>
#include <common/rdrnd.h>
#include <gt/async.h>
#include <io/file.h>
#include <net/rpc_response.h>
#include <tyrdbs/writer.h>
#include <tyrdbs/slice_writer.h>
#include <tyrdbs/overwrite_iterator.h>
#include <tyrdbs/meta_node/log_module.h>


namespace tyrtech::tyrdbs::meta_node::log {


class file_reader : public tyrdbs::reader
{
public:
    uint32_t pread(uint64_t offset, char* data, uint32_t size) const override
    {
        auto res = m_file.pread(offset, data, size);
        assert(static_cast<uint32_t>(res) == size);

        return res;
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
            auto res = m_file->pwrite(m_offset, data, size);
            assert(static_cast<uint32_t>(res) == size);

            m_offset += res;

            return res;
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


using writer_ptr =
        std::unique_ptr<slice_writer>;

using writers_t =
        std::unordered_map<uint16_t, writer_ptr>;

using slices_t =
        std::unordered_map<uint16_t, slice_ptr>;


bool load_block(const block_parser& block, writers_t* writers)
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
            auto fw = std::make_shared<file_writer>("data/{:016x}.dat", rdrnd());
            writer = std::make_unique<slice_writer>(std::move(fw));
        }

        bool eor = (flags & 0x01) != 0;
        bool deleted = (flags & 0x02) != 0;

        writer->add(key, value, eor, deleted);
    }

    return is_last_block;
}

writers_t load_writers(net::socket_channel* channel)
{
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

        message::parser parser(buffer.data(), *size + sizeof(uint16_t));
        is_last_block = load_block(block_parser(&parser, 0), &writers);
    }

    return writers;
}


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

    // send keys
}

void impl::update(const update::request_parser_t& request, context* ctx)
{
    while (m_slice_count > m_max_slices)
    {
        m_slice_count_cond.wait();
    }

    auto writers = load_writers(ctx->channel);
    slices_t slices;

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
                                                         std::move(reader));

            slices[ushard_id] = std::move(slice);
        };

        jobs.run(std::move(f));
    }

    jobs.wait();

    writers.clear();

    auto tid = m_next_tid++;

    for (auto& it : slices)
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

    net::rpc_response<log::update> response(ctx->channel);
    auto message = response.add_message();

    message.set_tid(tid);

    response.send();
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

impl::impl(const std::string_view& path,
           uint32_t merge_threads,
           uint32_t ushards,
           uint32_t max_slices)
  : m_max_slices(max_slices)
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
