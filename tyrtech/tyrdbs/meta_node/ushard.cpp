#include <common/branch_prediction.h>
#include <tyrdbs/meta_node/ushard.h>

#include <cassert>


namespace tyrtech::tyrdbs::meta_node {


bool ushard::add(uint8_t tier_id, std::shared_ptr<slice> slice)
{
    assert(likely(tier_id) < max_tiers);
    auto& tier = m_tiers[tier_id];

    tier.emplace_back(slice);

    return tier.size() > max_slices_per_tier;
}

bool ushard::remove(uint8_t tier_id, uint32_t count)
{
    assert(likely(tier_id) < max_tiers);
    auto& tier = m_tiers[tier_id];

    assert(likely(tier.size() >= count));
    tier.erase(tier.begin(), tier.begin() + count);

    return tier.size() > max_slices_per_tier;
}

slices_t ushard::get(uint8_t tier_id) const
{
    assert(likely(tier_id) < max_tiers);
    return m_tiers[tier_id];
}

slices_t ushard::get() const
{
    slices_t slices;

    for (auto& tier : m_tiers)
    {
        std::copy(tier.begin(), tier.end(), std::back_inserter(slices));
    }

    return slices;
}

bool ushard::needs_merge(uint8_t tier_id) const
{
    assert(likely(tier_id) < max_tiers);
    return m_tiers[tier_id].size() > max_slices_per_tier;
}

uint8_t ushard::tier_id_of(uint64_t key_count)
{
    if (key_count == 0)
    {
        return 0;
    }

    return (63 - __builtin_clzll(key_count)) >> 2;
}

ushard::ushard()
{
    m_tiers.resize(max_tiers);
}

}
