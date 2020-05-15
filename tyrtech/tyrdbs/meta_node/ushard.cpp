#include <tyrdbs/iterators/overwrite.h>
#include <tyrdbs/meta_node/ushard.h>


namespace tyrtech::tyrdbs::meta_node {


std::unique_ptr<iterator> ushard::range(const std::string_view& min_key,
                                        const std::string_view& max_key)
{
    return std::make_unique<iterators::overwrite>(min_key, max_key, get_slices());
}

std::unique_ptr<iterator> ushard::begin()
{
    return std::make_unique<iterators::overwrite>(get_slices());
}

void ushard::add(slice_ptr slice, meta_callback* cb)
{
    add(std::move(slice), cb, true);
}

uint64_t ushard::merge(slice_writer* target, uint32_t tier, meta_callback* cb)
{
    auto tier_slices = get_slices_for(tier);
    uint32_t count = tier_slices.size();

    if (count <= max_slices_per_tier)
    {
        return 0;
    }

    auto source_key_count = key_count(tier_slices);

    iterators::overwrite it(tier_slices);

    target->add(&it, false);
    target->flush();

    auto slice = std::make_shared<tyrdbs::slice>(target->commit(), "{}", target->path());

    add(std::move(slice), cb);
    remove_from(tier, count, cb);

    cb->remove(tier_slices);

    return source_key_count;
}

uint64_t ushard::compact(slice_writer* target, meta_callback* cb)
{
    auto slices = get_slices();

    if (slices.size() < 2)
    {
        return 0;
    }

    auto source_key_count = key_count(slices);

    tier_map_t tier_map_checkpoint = m_tier_map;

    iterators::overwrite it(slices);

    target->add(&it, true);
    target->flush();

    auto slice = std::make_shared<tyrdbs::slice>(target->commit(), "{}", target->path());

    add(std::move(slice), cb);

    for (auto&& it : tier_map_checkpoint)
    {
        remove_from(it.first, it.second.size(), cb);
    }

    cb->remove(slices);

    return source_key_count;
}

void ushard::drop()
{
    m_dropped = true;
}

slices_t ushard::get_slices() const
{
    slices_t slices;

    for (auto&& it : m_tier_map)
    {
        std::copy(it.second.begin(),
                  it.second.end(),
                  std::back_inserter(slices));
    }

    return slices;
}

ushard::~ushard()
{
    if (m_dropped == false)
    {
        return;
    }

    for (auto&& slice : get_slices())
    {
        slice->unlink();
    }

    m_tier_map.clear();
}

uint32_t ushard::tier_of(const slice_ptr& slice)
{
    return (64 - __builtin_clzll(slice->key_count())) >> 2;
}

slices_t ushard::get_slices_for(uint32_t tier)
{
    return m_tier_map[tier];
}

uint64_t ushard::key_count(const slices_t& slices)
{
    uint64_t key_count = 0;

    for (auto&& slice : slices)
    {
        key_count += slice->key_count();
    }

    return key_count;
}

void ushard::add(slice_ptr slice, meta_callback* cb, bool use_add_callback)
{
    if (use_add_callback == true)
    {
        cb->add(slice);
    }

    uint32_t tier = tier_of(slice);
    auto&& it = m_tier_map.find(tier);

    if (it == m_tier_map.end())
    {
        it = m_tier_map.insert(tier_map_t::value_type(tier, slices_t())).first;
    }

    it->second.emplace_back(std::move(slice));

    if (it->second.size() > max_slices_per_tier)
    {
        cb->merge(tier);
    }
}

void ushard::remove_from(uint32_t tier, uint32_t count, meta_callback* cb)
{
    auto& tier_slices = m_tier_map[tier];

    tier_slices.erase(tier_slices.begin(), tier_slices.begin() + count);

    if (tier_slices.size() > max_slices_per_tier)
    {
        cb->merge(tier);
    }
}

}
