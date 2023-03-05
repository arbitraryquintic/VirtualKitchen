/* Create a daily report including:
- Total unique sessions
- The average length of sessions in seconds
- The average number of searches completed before displaying a recipe 
- The ID of the recipe that was most viewed 

-- My table mock up:
|    day   |  total unique sessions  | avg session length (s) | avg number searches | most viewed recipe id |
 ---------- ------------------------  ------------------------ --------------------- -----------------------
| 01/01/01 | 			8		     |          100 		  |          4	        |   39q4r4wespijfd		|


I originally wanted to add recipe names but the table scan of vk_data.chefs.recipe was the most expensive node with a 900ms query execution time.
This deliverable did not require recipe names, so I removed it and my query took 312ms.
In the sessions CTE, I changed the case-when statement to an iff statement (which only allows a single condition) shaving down query executed to 284ms.
*/


--ALTER SESSION SET USE_CACHED_RESULT=FALSE;

with 
events as (
	select distinct 
    	event_id
        , session_id
        , event_timestamp
        , trim(parse_json(event_details):event, '*') as event_type
        , trim(parse_json(event_details):recipe_id, '*') as recipe_id
    from vk_data.events.website_activity
    order by session_id, event_timestamp
),
sessions as (
	select 
        session_id
        , to_date(min(event_timestamp)) as session_start_date
        , abs(datediff('seconds', max(event_timestamp), min(event_timestamp))) as session_duration_in_seconds
        --, case when count_if(event_type = 'view_recipe') = 0 then null
        --	else count_if(event_type = 'search')/count_if(event_type = 'view_recipe') end as num_searches_per_recipe_view
        , iff(count_if(event_type = 'view_recipe') = 0, null, 
        	  count_if(event_type = 'search')/count_if(event_type = 'view_recipe')) as num_searches_per_recipe_view
    from events
    group by session_id 
), 
most_viewed_recipe as (
    select 
    	to_date(event_timestamp) as event_day
        , recipe_id
        , count(*) as recipe_views 
    from events
    where recipe_id is not null
    group by to_date(event_timestamp), recipe_id
    qualify row_number() over (partition by event_day order by recipe_views desc) = 1 
)
select 
	session_start_date
    , count(distinct session_id) as total_sessions
	  , round(avg(session_duration_in_seconds)) as avg_session_duration_in_seconds
    , avg(num_searches_per_recipe_view) as avg_searches_per_recipe_view
    , max(most_viewed_recipe.recipe_id) as recipe_id
from sessions 
left join most_viewed_recipe
	on sessions.session_start_date = most_viewed_recipe.event_day
group by session_start_date
;
