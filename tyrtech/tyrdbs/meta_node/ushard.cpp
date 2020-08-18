#include <common/branch_prediction.h>
#include <common/rdrnd.h>
#include <gt/engine.h>
#include <tyrdbs/slice_writer.h>
#include <tyrdbs/meta_node/ushard.h>
#include <tyrdbs/meta_node/file_reader.h>
#include <tyrdbs/meta_node/file_writer.h>

#include <cassert>


namespace tyrtech::tyrdbs::meta_node {


void ushard::add(std::shared_ptr<slice> slice)
{
    auto tier_id = tier_id_of(slice->key_count());
    auto& tier = m_tiers[tier_id];

    tier.emplace_back(slice);

    if (tier.size() > max_slices_per_tier)
    {
        request_merge(tier_id);
    }
}

overwrite_iterator ushard::begin()
{
    return overwrite_iterator{get_all()};
}

ushard::ushard(const std::string_view& path)
  : m_path{path}
{
    m_tiers.resize(max_tiers);

    m_filter.resize(max_tiers);
    m_locks.resize(max_tiers);
}

uint8_t ushard::tier_id_of(uint64_t key_count)
{
    if (key_count == 0)
    {
        return 0;
    }

    return (63 - __builtin_clzll(key_count)) >> 2;
}

slices_t ushard::get_all() const
{
    slices_t slices;

    for (auto& tier : m_tiers)
    {
        std::copy(tier.begin(), tier.end(), std::back_inserter(slices));
    }

    return slices;
}

void ushard::remove(uint8_t tier_id, uint32_t count)
{
    assert(likely(tier_id) < max_tiers);
    auto& tier = m_tiers[tier_id];

    assert(likely(tier.size() >= count));
    tier.erase(tier.begin(), tier.begin() + count);

    if (tier.size() > max_slices_per_tier)
    {
        request_merge(tier_id);
    }
}

void ushard::request_merge(uint8_t tier_id)
{
    if (m_filter[tier_id] == true)
    {
        return;
    }

    m_filter[tier_id] = true;

    gt::create_thread(&ushard::merge, this, tier_id);
}

void ushard::merge(uint8_t tier_id)
{
    assert(likely(tier_id) < max_tiers);

    assert(m_filter[tier_id] == true);
    m_filter[tier_id] = false;

    if (m_locks[tier_id] == true)
    {
        return;
    }

    m_locks[tier_id] = true;

    {
        if (m_tiers[tier_id].size() <= max_slices_per_tier)
        {
            return;
        }

        auto slices = m_tiers[tier_id];

        auto fw = std::make_shared<file_writer>("{}/{:016x}.dat", m_path, rdrnd());
        auto writer = std::make_unique<slice_writer>(fw);

        auto it = overwrite_iterator(slices);

        writer->add(&it, false);

        writer->flush();
        writer->commit();

        auto reader = std::make_shared<file_reader>(fw->path());
        auto slice = std::make_shared<tyrdbs::slice>(fw->offset(), std::move(reader));

        //slice->set_cache_id(m_next_cache_id++);

        remove(tier_id, slices.size());
        add(std::move(slice));

        //assert(m_slice_count >= slices.size());
        //m_slice_count -= slices.size();

        //m_slice_count++;

        //if (m_slice_count <= m_max_slices)
        //{
        //    m_slice_count_cond.signal_all();
        //}

        for (auto& s : slices)
        {
            s->unlink();
        }
    }

    m_locks[tier_id] = false;
}

}
