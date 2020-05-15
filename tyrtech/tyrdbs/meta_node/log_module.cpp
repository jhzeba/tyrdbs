#include <gt/async.h>
#include <tyrdbs/meta_node/log_module.h>
#include <tyrdbs/meta_node/data.json.h>


namespace tyrtech::tyrdbs::meta_node::log {


struct meta_callback : public ushard::meta_callback
{
    void add(const slice_ptr& slice) override
    {
    }

    void remove(const slices_t& slices) override
    {
        for (auto&& slice : slices)
        {
            slice->unlink();
        }
    }

    void merge(uint16_t tier) override
    {
    }
};


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

impl::context impl::create_context(const std::shared_ptr<io::channel>& remote)
{
    return context(this);
}

uint64_t impl::context::next_handle()
{
    return m_next_handle++;
}

impl::context::writers_t::iterator impl::context::get_writer(uint64_t handle)
{
    auto it = m_writers.find(handle);

    if (it == m_writers.end())
    {
        throw invalid_handle_exception("{:016x}: handle not found", handle);
    }

    return it;
}

impl::context::readers_t::iterator impl::context::get_reader(uint64_t handle)
{
    auto it = m_readers.find(handle);

    if (it == m_readers.end())
    {
        throw invalid_handle_exception("{:016x}: handle not found", handle);
    }

    return it;
}

void impl::update(const update::request_parser_t& request,
                  update::response_builder_t* response,
                  context* ctx)
{
    if (request.has_handle() == false)
    {
        context::writer w;

        update_data(request.get_parser(), request.data(), &w);

        auto handle = ctx->next_handle();

        ctx->m_writers[handle] = std::move(w);
        response->add_handle(handle);
    }
    else
    {
        auto handle = request.handle();
        auto it = ctx->get_writer(handle);

        update_data(request.get_parser(), request.data(), &it->second);
    }
}

void impl::commit(const commit::request_parser_t& request,
                  commit::response_builder_t* response,
                  context* ctx)
{
    if (request.has_handle() == false)
    {
        return;
    }

    auto handle = request.handle();
    auto it = ctx->get_writer(handle);

    for (auto& it : it->second.slice_writers)
    {
        meta_callback cb;

        auto slice = std::make_shared<tyrdbs::slice>(it.second->commit(),
                                                     it.second->path());
        slice->set_tid(m_next_tid++);

        m_ushards[it.first % m_ushards.size()]->add(std::move(slice), &cb);
    }

    ctx->m_writers.erase(it);
}

void impl::rollback(const rollback::request_parser_t& request,
                    rollback::response_builder_t* response,
                    context* ctx)
{
    if (request.has_handle() == false)
    {
        return;
    }

    auto handle = request.handle();
    auto it = ctx->get_writer(handle);

    ctx->m_writers.erase(it);
}

void impl::fetch(const fetch::request_parser_t& request,
                 fetch::response_builder_t* response,
                 context* ctx)
{
    if (request.has_handle() == false)
    {
        auto ushard = m_ushards[request.ushard() % m_ushards.size()];
        auto it = ushard->range(request.min_key(), request.max_key());

        if (it->next() == true)
        {
            context::reader r;

            r.iterator = std::move(it);
            r.value_part = r.iterator->value();

            if (fetch_entries(&r, response->add_data()) == false)
            {
                auto handle = ctx->next_handle();

                ctx->m_readers[handle] = std::move(r);
                response->add_handle(handle);
            }
        }
    }
    else
    {
        auto handle = request.handle();
        auto it = ctx->get_reader(handle);

        if (fetch_entries(&it->second, response->add_data()) == true)
        {
            ctx->m_readers.erase(handle);
        }
        else
        {
            response->add_handle(handle);
        }
    }
}

void impl::abort(const abort::request_parser_t& request,
                 abort::response_builder_t* response,
                 context* ctx)
{
    if (request.has_handle() == false)
    {
        return;
    }

    auto handle = request.handle();
    auto it = ctx->get_reader(handle);

    ctx->m_readers.erase(it);
}

bool impl::fetch_entries(context::reader* r, message::builder* builder)
{
    uint8_t data_flags = 0;

    auto data = data_builder(builder);

    auto ushards = data.add_ushards();
    auto ushard = ushards.add_value();
    auto entries = ushard.add_entries();

    while (true)
    {
        auto key = r->iterator->key();

        uint8_t entry_flags = 0;

        if (r->iterator->eor() == true)
        {
            entry_flags |= 0x01;
        }

        if (r->iterator->deleted() == true)
        {
            entry_flags |= 0x02;
        }

        uint16_t bytes_required = 0;

        bytes_required += entry_builder::bytes_required();
        bytes_required += entry_builder::key_bytes_required();
        bytes_required += entry_builder::value_bytes_required();
        bytes_required += key.size();

        bytes_required += fetch::response_builder_t::handle_bytes_required();

        if (builder->available_space() <= bytes_required)
        {
            break;
        }

        assert(r->value_part.size() < 65536);

        uint16_t part_size = builder->available_space();
        part_size -= bytes_required;

        part_size = std::min(part_size, static_cast<uint16_t>(r->value_part.size()));

        if (part_size != r->value_part.size())
        {
            entry_flags &= ~0x01;
        }

        auto entry = entries.add_value();

        entry.set_flags(entry_flags);
        entry.add_key(key);
        entry.add_value(r->value_part.substr(0, part_size));

        if (part_size != r->value_part.size())
        {
            r->value_part = r->value_part.substr(part_size);
            break;
        }

        if (r->iterator->next() == false)
        {
            data_flags |= 0x01;
            break;
        }

        r->value_part = r->iterator->value();
    }

    data.set_flags(data_flags);

    return (data_flags & 0x01) == 0x01;
}

void impl::update_data(const message::parser* p, uint16_t off, context::writer* w)
{
    data_parser data(p, off);

    if (data.has_ushards() == true)
    {
        auto ushards = data.ushards();

        while (ushards.next() == true)
        {
            auto ushard = ushards.value();
            auto& slice_writer = w->slice_writers[ushard.id() % m_ushards.size()];

            if (slice_writer == nullptr)
            {
                slice_writer = std::make_unique<tyrdbs::slice_writer>();
            }

            if (ushard.has_entries() == false)
            {
                continue;
            }

            auto entries = ushard.entries();

            while (entries.next() == true)
            {
                auto entry = entries.value();

                bool eor = entry.flags() & 0x01;
                bool deleted = entry.flags() & 0x02;

                slice_writer->add(entry.key(), entry.value(), eor, deleted);
            }
        }
    }

    if ((data.flags() & 0x01) == 0x00)
    {
        return;
    }

    auto jobs = gt::async::create_jobs();

    for (auto&& it : w->slice_writers)
    {
        auto f = [it]
        {
            it.second->flush();
        };

        jobs.run(std::move(f));
    }

    jobs.wait();
}

impl::impl(const std::string_view& path, uint32_t ushards)
{
    for (uint32_t i = 0; i < ushards; i++)
    {
        m_ushards.push_back(std::make_shared<ushard>());
    }
}

}
