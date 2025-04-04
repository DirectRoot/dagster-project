with 
    e as (
        select *
        from {{ source('okta_logs', 'log_events') }}
        where event_type = 'user.lifecycle.activate'
    ),

    t as (
        select *
        from {{ source('okta_logs', 'log_events__target') }}
    )

select t.display_name as user_name, e.actor__display_name as created_by
from e
left join t on e._dlt_id = t._dlt_parent_id