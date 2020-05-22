#pragma once


#include <tyrdbs/slice.h>


namespace tyrtech::tyrdbs::meta_node {


class ushard : private disallow_copy
{
public:
    static constexpr uint32_t max_tiers{16};
    static constexpr uint32_t max_slices_per_tier{4};

public:
    bool add(uint8_t tier_id, std::shared_ptr<slice> slice);
    bool remove(uint8_t tier_id, uint32_t count);

    slices_t get(uint8_t tier_id) const;
    slices_t get() const;

    bool needs_merge(uint8_t tier_id) const;

public:
    static uint8_t tier_id_of(uint64_t key_count);

public:
    ushard();

private:
    using tiers_t =
            std::vector<slices_t>;

private:
    tiers_t m_tiers;
};

}
