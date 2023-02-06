-- ##############################################################################################################################################################################
-- Exercise 1: 
-- We have 10,000 potential customers who have signed up with Virtual Kitchen. If the customer is able to order from us, then their city/state will be present in our database. 
-- Create a query in Snowflake that returns all customers that can place an order with Virtual Kitchen.
-- ##############################################################################################################################################################################

-- EXPLORE
-- looks like there are some city-state duplicates with differing county names and geo coordinates, will need to pick one
select * 
from (
    select 
        city_name, 
        state_abbr,
        county_name,
        geo_location,
        count(*) over (partition by city_name, state_abbr) as freqs
    from vk_data.resources.us_cities 
    order by freqs desc, city_name, state_abbr
) city_dup_count
where city_dup_count.freqs > 1;


-- SOLUTION
    -- clean up us cities table
    -- gather customers data in one table and add geolocation data from us cities 
    -- add geolocation data to suppliers data from us cities table
    -- to calculate distances between customer locations and supplier locations, join the info together via crossjoin and subtract distances
    -- pick the minimum distance between customer and supplier 

with us_cities_clean as (
-- remove dups from US cities table
    select 
        city_id,
        upper(trim(city_name)) as city,
        upper(trim(state_abbr)) as state,
        geo_location
    from vk_data.resources.us_cities   
    qualify row_number() over (partition by city_name, state_abbr order by county_name) = 1
)
, customers as (
-- select required columns for customers
    select
        cd.customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name,
        email as customer_email,
        customer_city,
        customer_state,
        geo_location as customer_geo_location
    from vk_data.customers.customer_data cd
    inner join vk_data.customers.customer_address ca
        using (customer_id)
    inner join us_cities_clean ci
        on (upper(trim(ca.customer_city)) = upper(trim(ci.city))
        and upper(trim(ca.customer_state)) = upper(trim(ci.state))
            )
)
, suppliers as (
    select 
        supplier_id,
        supplier_name,
        upper(trim(s.supplier_city)) as supplier_city,
        upper(trim(s.supplier_state)) as supplier_state,
        geo_location as supplier_geo_location 
from VK_DATA.SUPPLIERS.SUPPLIER_INFO as s
left join us_cities_clean as us
    on upper(trim(s.supplier_city)) = upper(trim(us.city))
    and upper(trim(s.supplier_state)) = upper(trim(us.state))     
)
, final_table as (
    select
        c.customer_id, 
        c.customer_first_name,
        c.customer_last_name,
        c.customer_email,
        s.supplier_id, 
        s.supplier_name,
        abs(st_distance(customer_geo_location, supplier_geo_location) / 1000) as distance_km
        --min(distance_km) over (partition by c.customer_id) as min_distance
    from customers c
    cross join suppliers s    
    qualify min(distance_km) over (partition by c.customer_id) = distance_km

)
select * from final_table;
