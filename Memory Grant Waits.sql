/*
select *
from sys.dm_exec_query_resource_semaphores

select *
from sys.dm_resource_governor_resource_pools

select *
from sys.dm_resource_governor_workload_groups
*/


select
    a.wait_order,
	db_name(d.database_id) as db,
	d.login_name,
    b.[text],
    c.query_plan,
    convert(decimal(19,2), wait_time_ms / 1000. / 60.) as wait_time_minutes,
    convert(decimal(19,2), a.required_memory_kb / 1024.) as required_MB,
    convert(decimal(19,2), a.requested_memory_kb / 1024.) as requested_MB,
    convert(decimal(19,2), a.ideal_memory_kb / 1024.) as ideal_MB,
    convert(decimal(19,2), a.granted_memory_kb / 1024.) as granted_MB,
    convert(decimal(19,2), a.used_memory_kb / 1024.) as used_MB,
    a.*
from sys.dm_exec_query_memory_grants a
cross apply sys.dm_exec_sql_text(a.[sql_handle]) b
cross apply sys.dm_exec_query_plan(a.plan_handle) c
left outer join sys.dm_exec_sessions d
	on d.session_id = a.session_id
where b.[text] not like '%sys.dm_exec_query_memory_grants%'
    and b.[text] not like '%SQL diagnostic manager%'
order by
    a.wait_order,
	a.is_next_candidate desc