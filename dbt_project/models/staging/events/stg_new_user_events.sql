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

select e.*, t.*
from e
left join t on e._dlt_id = t._dlt_parent_id