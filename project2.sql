-- ##############################################################################################################################################################################
/* Exercise : 
Virtual Kitchen has an emergency! 

We shipped several meal kits without including fresh parsley, and our customers are starting to complain. We have identified the impacted cities, and we know that 25 of our 
customers did not get their parsley. That number might seem small, but Virtual Kitchen is committed to providing every customer with a great experience.

Our management has decided to provide a different recipe for free (if the customer has other preferences available), or else use grocery stores in the greater Chicago area to 
send an overnight shipment of fresh parsley to our customers. We have one store in Chicago, IL and one store in Gary, IN both ready to help out with this request.

Last night, our on-call developer created a query to identify the impacted customers and their attributes in order to compose an offer to these customers to make things right. 
But the developer was paged at 2 a.m. when the problem occurred, and she created a fast query so that she could go back to sleep.

You review her code today and decide to reformat her query so that she can catch up on sleep.*/
-- ##############################################################################################################################################################################

-- Refactored Code Solution:
    -- identify impacted customers based on state and city
    -- find impacted customers geocodes for later distance calculation from Chicago and Gary
    -- identify food preferences for active customers 
    -- find geocode for Chicago and geocode for Gary
    -- put it altogether and calculate distance from customer city to Virtual Kitchen stores

with 
    impacted_customers as (
        select 
           customer_id
            , customer_city
            , customer_state
        from vk_data.customers.customer_address
        where 
           (customer_state = 'KY'
            and (trim(customer_city) ilike '%concord%' 
            or trim(customer_city) ilike '%georgetown%' 
            or trim(customer_city) ilike '%ashland%')
            )
        	or (customer_state = 'CA' 
            	and (trim(customer_city) ilike '%oakland%' 
                or trim(customer_city) ilike '%pleasant hill%')
            )
            /* --original code with confusing and-or order of operations, brownsville does not need to be in TX
            or (customer_state = 'TX' and (trim(customer_city) ilike '%arlington%') or trim(customer_city) ilike '%brownsville%' */
            or (customer_state = 'TX' 
            	and (trim(customer_city) ilike '%arlington%') 
            ) -- clearer syntax, brownsville does not have to be in TX
            or trim(customer_city) ilike '%brownsville%'
    ),
    impacted_customers_with_geocodes as (
	select 
            ic.*
            , us.geo_location
        from impacted_customers as ic 
        -- we only want customers with a geocode we can match to the us cities dataset so we can calculate distance for
        inner join vk_data.resources.us_cities us
        	on lower(trim(ic.customer_state)) = lower(trim(us.state_abbr))
            and lower(trim(ic.customer_city)) = lower(trim(us.city_name))
    
    ),
	count_food_pref_by_active_customer as ( 
    	select 
            customer_id
            , count(*) as food_pref_count
        from vk_data.customers.customer_survey
        where is_active = true
        group by 1
	),
    geo_chicago as (
    	select 
            geo_location as geo_chicago
    	from vk_data.resources.us_cities 
    	where lower(trim(city_name)) = 'chicago' and lower(trim(state_abbr)) = 'il'
    ),
    geo_gary as (
    	select 
            geo_location as geo_gary
    	from vk_data.resources.us_cities 
    	where lower(trim(city_name)) = 'gary' and lower(trim(state_abbr)) = 'in'
    ) 
    select 
    	cd.first_name || ' ' || cd.last_name as customer_name
        , ic.customer_city
        , ic.customer_state
        , fp.food_pref_count
        , (st_distance(ic.geo_location, chi.geo_chicago) / 1609)::int as chicago_distance_miles
        , (st_distance(ic.geo_location, gary.geo_gary) / 1609)::int as gary_distance_miles
    from vk_data.customers.customer_data as cd 
    inner join impacted_customers_with_geocodes as ic on cd.customer_id = ic.customer_id
    inner join count_food_pref_by_active_customer as fp on ic.customer_id = fp.customer_id
    cross join geo_chicago as chi
    cross join geo_gary as gary
;
