WITH suppliers_data AS (
    SELECT
        vk_data.suppliers.supplier_info.supplier_id,
        vk_data.suppliers.supplier_info.supplier_name,
        vk_data.suppliers.supplier_info.supplier_city,
        vk_data.suppliers.supplier_info.supplier_state,
        vk_data.resources.us_cities.lat,
        vk_data.resources.us_cities.long
    FROM
        vk_data.suppliers.supplier_info
    LEFT JOIN vk_data.resources.us_cities ON
        upper(
            vk_data.suppliers.supplier_info.supplier_city
        ) = vk_data.resources.us_cities.city_name
        AND upper(
            vk_data.suppliers.supplier_info.supplier_state
        ) = vk_data.resources.us_cities.state_abbr
),

cities AS (

    SELECT
        vk_data.resources.us_cities.city_name,
        vk_data.resources.us_cities.state_abbr,
        vk_data.resources.us_cities.lat,
        vk_data.resources.us_cities.long
    FROM vk_data.resources.us_cities
    QUALIFY
        row_number() OVER (
            PARTITION BY
                vk_data.resources.us_cities.city_name,
                vk_data.resources.us_cities.state_abbr
            ORDER BY vk_data.resources.us_cities.county_name
        ) = 1
),

customers_data AS (
    SELECT
        vk_data.customers.customer_data.customer_id,
        vk_data.customers.customer_data.first_name,
        vk_data.customers.customer_data.last_name,
        vk_data.customers.customer_address.customer_city,
        vk_data.customers.customer_address.customer_state,
        vk_data.customers.customer_address.customer_postal_code,
        cities.lat,
        cities.long,
        vk_data.customers.customer_data.email
    FROM vk_data.customers.customer_data
    INNER JOIN
        vk_data.customers.customer_address ON
            vk_data.customers.customer_data.customer_id = vk_data.customers.customer_address.customer_id
    INNER JOIN cities ON
        (
            (
                upper(
                    trim(vk_data.customers.customer_address.customer_city)
                ) = upper(cities.city_name)
                AND upper(
                    trim(vk_data.customers.customer_address.customer_state)
                ) = upper(cities.state_abbr)
            ))


)
,
distances AS (
    SELECT
        customer_id,
        customers_data.first_name,
        customers_data.last_name,
        supplier_id,
        suppliers_data.supplier_name,
        suppliers_data.lat,
        customers_data.lat,
        suppliers_data.long,
        customers_data.long,
        customers_data.customer_city,
        customers_data.customer_state,
        suppliers_data.supplier_city,
        suppliers_data.supplier_state,
        customers_data.email,
        (st_distance(
            st_makepoint(customers_data.long, customers_data.lat),
            st_makepoint(suppliers_data.long, suppliers_data.lat)

            ) / 1609) AS dist_in_kilometres
    --, sqrt(SQUARE(sd.LAT-cd.LAT) + SQUARE(sd.LONG-cd.LONG)) distance
    FROM suppliers_data CROSS JOIN customers_data
    QUALIFY
        row_number() OVER (
            PARTITION BY customer_id ORDER BY dist_in_kilometres
        ) = 1
)


SELECT
    customer_id AS "Customer ID",
    first_name AS "Customer first name",
    last_name AS "Customer last name",
    email AS "Customer email",
    supplier_id AS "Supplier ID",
    supplier_name AS "Supplier name",
    dist_in_kilometres AS "Shipping distance Miles"
FROM distances ORDER BY 3, 2;
