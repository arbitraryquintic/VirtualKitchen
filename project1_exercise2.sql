-- ##############################################################################################################################################################################
-- Exercise 2: 
-- Now that we know which customers can order from Virtual Kitchen, we want to launch an email marketing campaign to let these customers know that they can order from our website. 
-- If the customer completed a survey about their food interests, then we also want to include up to three of their choices in a personalized email message.
-- ##############################################################################################################################################################################



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
, eligible as (
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

) -- ^^ From Exercise 1
-- SOLUTION
    -- bring in and clean up tags for each customer, subset to only 3 using row number index
    -- pivot tags to make data one row per customer 
    -- flatten array of recipe tags to long format and keep only 1 recipe per tag
    -- join suggested recipe to customer tags on the first food preference column
, customer_tags as ( 
    select 
        e.customer_id, 
        e.customer_email,
        e.customer_first_name,
        trim(t.tag_property) as tag_property,
        row_number() over (partition by customer_id order by tag_property) as rownumber
    from eligible e 
    inner join vk_data.customers.customer_survey s
        using (customer_id) 
    left join vk_data.resources.recipe_tags t
        using (tag_id) 
    where is_active = 'TRUE'
    qualify rownumber < 4  
)
, pivot_customer_tags as ( 
    select *
    from customer_tags
    pivot( max(tag_property) 
    for rownumber in (1, 2, 3)) 
    as pivot_values (customer_id, customer_email, customer_first_name, food_pref_1, food_pref_2, food_pref_3)

)
, recipe_tags as (
    select 
        recipe_name as suggested_recipe,
        trim(flat_tags.value, '" ') as tag_value
    from vk_data.chefs.recipe
    , table(flatten(tag_list)) as flat_tags
    qualify row_number() over (partition by tag_value order by suggested_recipe) = 1
    order by tag_value
    
)
, final_table as (
    select 
        c.*,
        r.suggested_recipe
    from pivot_customer_tags c
    left join recipe_tags r
        on upper(trim(c.food_pref_1)) = upper(trim(r.tag_value))
    order by customer_email
    
)
select * from final_table;
