with search_data as (
    select
        session_id,
        event_timestamp::date as event_date,
        event_timestamp,
        JSON_EXTRACT_PATH_TEXT(
            vk_data.events.website_activity.event_details, 'event'
        ) as visit_type,
        JSON_EXTRACT_PATH_TEXT(
            vk_data.events.website_activity.event_details, 'recipe_id'
        ) as recipe_id,
        COUNT(
            recipe_id
        ) over (partition by event_timestamp::date, recipe_id) as view_count
    from vk_data.events.website_activity
    where
        JSON_EXTRACT_PATH_TEXT(
            vk_data.events.website_activity.event_details, 'event'
        ) in ('search', 'view_recipe')
)
,
search_per_recipe as (select
    *,
    COUNT(
        *
    ) over (
        partition by
            event_date, session_id
        order by
            event_timestamp
        rows between unbounded preceding and 1 preceding
    ) as searches,
    ROW_NUMBER() over (
        partition by event_date order by view_count desc
    ) as views_rank
    from search_data
--WHERE visit_type IN ('search', 'view_recipe')
)
,
session_raw as (
    select
        session_id,
        MIN(
            vk_data.events.website_activity.event_timestamp
        )::date as event_date,
        DATEDIFF(
            'seconds',
            MIN(vk_data.events.website_activity.event_timestamp),
            MAX(vk_data.events.website_activity.event_timestamp)
        ) as session_duration
    from vk_data.events.website_activity
    group by session_id
)

select
    session_raw.event_date,
    COUNT(distinct session_raw.session_id) as total_sessions,
    ROUND(AVG(session_duration)) as average_session_duration,
    AVG(searches) as avg_searches,
    MIN(
        case when views_rank = 1 then search_per_recipe.recipe_id end
    ) as recipe,
    MIN(vk_data.chefs.recipe.recipe_name) as recipe_name
from session_raw
inner join
    search_per_recipe on session_raw.event_date = search_per_recipe.event_date
inner join
    vk_data.chefs.recipe on
        search_per_recipe.recipe_id = vk_data.chefs.recipe.recipe_id
where visit_type = 'view_recipe'
group by session_raw.event_date
ORDER BY session_raw.event_date
