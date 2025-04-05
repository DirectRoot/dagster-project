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

select
    e.uuid,
    e.published,
	t.display_name as user_name,
	t.id as user_id,
	e.actor__display_name as created_by,
	e.outcome__result as outcome_result,
	e.outcome__reason as outcome_reason
from e
left join t on e._dlt_id = t._dlt_parent_id