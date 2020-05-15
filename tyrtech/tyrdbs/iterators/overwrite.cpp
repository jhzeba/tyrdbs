#include <gt/async.h>
#include <tyrdbs/iterators/overwrite.h>


namespace tyrtech::tyrdbs::iterators {


bool overwrite::next()
{
    if (m_elements.size() == 0)
    {
        return false;
    }

    if (m_last_key.size() == 0)
    {
        std::sort(m_elements.begin(), m_elements.end(), cmp());
        m_last_key.assign(m_elements.back().second->key());

        if (is_out_of_bounds() == true)
        {
            m_elements.clear();
            return false;
        }

        return true;
    }

    if (eor() == false)
    {
        bool has_next = advance_last();
        assert(likely(has_next == true));

        return true;
    }

    while (advance() == true)
    {
        if (is_out_of_bounds() == true)
        {
            m_elements.clear();
            return false;
        }

        if (key().compare(m_last_key.data()) != 0)
        {
            m_last_key.assign(key());
            return true;
        }
    }

    return false;
}

std::string_view overwrite::key() const
{
    return m_elements.back().second->key();
}

std::string_view overwrite::value() const
{
    return m_elements.back().second->value();
}

bool overwrite::eor() const
{
    return m_elements.back().second->eor();
}

bool overwrite::deleted() const
{
    return m_elements.back().second->deleted();
}

uint64_t overwrite::tid() const
{
    return m_elements.back().second->tid();
}

overwrite::overwrite(const std::string_view& min_key,
                   const std::string_view& max_key,
                   slices_t slices)
{
    m_elements.reserve(slices.size());

    auto jobs = gt::async::create_jobs();

    for (auto&& slice : slices)
    {
        auto f = [this, slice = std::move(slice), &min_key, &max_key]
        {
            auto&& it = slice->range(min_key, max_key);

            if (it == nullptr)
            {
                return;
            }

            if (it->next() == false)
            {
                return;
            }

            if (it->key().compare(max_key) > 0)
            {
                return;
            }

            this->m_elements.emplace_back(element_t(std::move(slice),
                                                    std::move(it)));
        };

        jobs.run(std::move(f));
    }

    jobs.wait();

    if (m_elements.size() != 0)
    {
        m_max_key.assign(max_key);
    }
}

overwrite::overwrite(slices_t slices)
{
    m_elements.reserve(slices.size());

    for (auto&& slice : slices)
    {
        auto&& it = slice->begin();

        if (it == nullptr)
        {
            continue;
        }

        if (it->next() == true)
        {
            this->m_elements.emplace_back(element_t(std::move(slice),
                                                    std::move(it)));
        }
    }
}

bool overwrite::is_out_of_bounds()
{
    if (m_max_key.size() == 0)
    {
        return false;
    }

    return key().compare(m_max_key.data()) > 0;
}

bool overwrite::advance_last()
{
    return m_elements.back().second->next();
}

bool overwrite::advance()
{
    bool has_next = advance_last();

    if (has_next == false)
    {
        m_elements.pop_back();
        return m_elements.size() != 0;
    }

    auto&& it = std::lower_bound(m_elements.begin(),
                                 m_elements.end(),
                                 m_elements.back(),
                                 cmp());

    if (it != --m_elements.end())
    {
        element_t e = std::move(m_elements.back());
        m_elements.insert(it, std::move(e));

        m_elements.pop_back();
    }

    return true;
}

}
