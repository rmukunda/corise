--get details for customers eligible for ordering. Also get the top 3 tags by tag name for each of them using ROW_NUMBER window function
WITH cust_tags AS (
    SELECT
        vk_data.customers.customer_data.customer_id,
        vk_data.customers.customer_data.first_name,
        vk_data.customers.customer_data.last_name,
        vk_data.customers.customer_data.email,
        trim(vk_data.resources.recipe_tags.tag_property) AS customer_tag,
        ROW_NUMBER () OVER (
            PARTITION BY
                vk_data.customers.customer_data.customer_id
            ORDER BY trim(vk_data.resources.recipe_tags.tag_property)
        ) AS tag_rank
    FROM vk_data.resources.recipe_tags
    INNER JOIN
        vk_data.customers.customer_survey ON
            vk_data.resources.recipe_tags.tag_id = vk_data.customers.customer_survey.tag_id AND is_active
    INNER JOIN
        vk_data.customers.customer_data ON
            vk_data.customers.customer_data.customer_id = vk_data.customers.customer_survey.customer_id
    INNER JOIN
        vk_data.customers.customer_address ON
            vk_data.customers.customer_data.customer_id = vk_data.customers.customer_address.customer_id
    INNER JOIN vk_data.resources.us_cities ON
        (
            upper(
                trim(vk_data.customers.customer_address.customer_city)
            ) = upper(vk_data.resources.us_cities.city_name)
            AND upper(
                trim(vk_data.customers.customer_address.customer_state)
            ) = upper(vk_data.resources.us_cities.state_abbr)
        )
    QUALIFY tag_rank <= 3
),
--Flatten the tags for all the recipes.
recipe_tags AS (SELECT
    recipe_name,
    trim(replace(t.value, '"', '')) AS tag_word
    FROM vk_data.chefs.recipe, table(flatten(vk_data.chefs.recipe.tag_list)) AS t
),
--Pivot the customer tag data to get the first 3 preferences as columns.
customer_prefs AS (SELECT * FROM
    cust_tags
    PIVOT(
        min(customer_tag) FOR tag_rank IN (1, 2, 3)
    ) AS pivot_values(
        customer_id,
        first_name,
        last_name,
        email,
        food_pref_1,
        food_pref_2,
        food_pref_3
    )
),
--Get a random recipe for the first food preference
customer_recipe AS (
    SELECT
        customer_id,
        any_value(recipe_tags.recipe_name) AS recipe
    FROM
        cust_tags
    INNER JOIN
        recipe_tags ON
            cust_tags.customer_tag = recipe_tags.tag_word AND cust_tags.tag_rank = 1
    GROUP BY 1
)

--Present the final data with customer preferences & recipe chosen.
SELECT
    customer_prefs.customer_id,
    first_name,
    last_name,
    email,
    food_pref_1,
    food_pref_2,
    food_pref_3,
    recipe AS suggested_recipe
FROM
    customer_prefs
LEFT JOIN
    customer_recipe ON customer_prefs.customer_id = customer_recipe.customer_id
ORDER BY email