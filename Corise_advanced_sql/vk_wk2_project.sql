with customer_details as (
    select
        customer_data.customer_id,
        customer_data.first_name || ' ' || customer_data.last_name as customer_name,
        trim(customer_address.customer_city) as customer_city,
        trim(customer_address.customer_state) as customer_state
    from vk_data.customers.customer_data as customer_data
    inner join vk_data.customers.customer_address as  customer_address
        on
            customer_data.customer_id = customer_address.customer_id
),

customer_food_pref as (
    select
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),

cities as (
    select
        cities.city_name,
        state_abbr,
        cities.geo_location,
        case
            when
                (state_abbr = 'KY' and trim(city_name) ilike any('%concord%', '%georgetown%', '%ashland%')) then true
            when
                (state_abbr = 'CA' and (trim(city_name) ilike any('%oakland%', '%pleasant hill%'))) then true
            when
                (state_abbr = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%') then true
            else false end as filter_criterion
    from vk_data.resources.us_cities as cities
),

gary as (
    select geo_location
    from cities
    where city_name = 'GARY' and state_abbr = 'IN'
),

chicago as (
    select geo_location
    from cities
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),

final_cte as (
    select
        customer_details.customer_name,
        customer_details.customer_city,
        customer_details.customer_state,
        customer_food_pref.food_pref_count,
        (
            st_distance(cities.geo_location, chicago.geo_location) / 1609
        )::int as chicago_distance_miles,
        (
            st_distance(cities.geo_location, gary.geo_location) / 1609
        )::int as gary_distance_miles
    from customer_details
    inner join
        customer_food_pref on
            customer_details.customer_id = customer_food_pref.customer_id
    left join cities
        on upper(customer_details.customer_state) = upper(cities.state_abbr)
            and lower(customer_details.customer_city) = lower(cities.city_name)
    cross join gary
    cross join chicago
    where filter_criterion
)

select *
from final_cte