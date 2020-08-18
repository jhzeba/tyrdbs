#pragma once


#include <tyrdbs/slice.h>
#include <tyrdbs/overwrite_iterator.h>


namespace tyrtech::tyrdbs::meta_node {


class ushard : private disallow_copy
{
public:
    static constexpr uint32_t max_tiers{16};
    static constexpr uint32_t max_slices_per_tier{2};

public:
    void add(std::shared_ptr<slice> slice);
    overwrite_iterator begin();

public:
    ushard(const std::string_view& path);

private:
    using tiers_t =
            std::vector<slices_t>;

    using bools_t =
            std::vector<bool>;

private:
    std::string_view m_path;

    tiers_t m_tiers;

    bools_t m_filter;
    bools_t m_locks;

private:
    static uint8_t tier_id_of(uint64_t key_count);

private:
    slices_t get_all() const;
    void request_merge(uint8_t tier_id);

    void remove(uint8_t tier_id, uint32_t count);

    void merge(uint8_t tier_id);
    void compact();
};

}
