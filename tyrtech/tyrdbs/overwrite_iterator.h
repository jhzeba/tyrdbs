#pragma once


#include <tyrdbs/iterator.h>
#include <tyrdbs/slice.h>
#include <tyrdbs/key_buffer.h>


namespace tyrtech::tyrdbs {


class overwrite_iterator : public iterator
{
public:
    bool next() override;

    std::string_view key() const override;
    std::string_view value() const override;

    bool eor() const override;
    bool deleted() const override;

    uint64_t tid() const override;

public:
    overwrite_iterator(const std::string_view& min_key,
                       const std::string_view& max_key,
                       slices_t slices);
    overwrite_iterator(slices_t slices);

private:
    using element_t =
            std::pair<std::shared_ptr<slice>, std::unique_ptr<iterator>>;

    using elements_t =
            std::vector<element_t>;

private:
    elements_t m_elements;

    key_buffer m_max_key;
    key_buffer m_last_key;

private:
    struct cmp
    {
        bool operator()(const element_t& e1, const element_t& e2)
        {
            auto& it1 = e1.second;
            auto& it2 = e2.second;

            int32_t cmp = it1->key().compare(it2->key());

            if (cmp == 0)
            {
                return it1->tid() < it2->tid();
            }

            return cmp > 0;
        }
    };

private:
    bool is_out_of_bounds();

    bool advance_last();
    bool advance();
};

}
