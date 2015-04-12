select
	db_name(a.database_id) as db,
	user_name(a.user_id) as username,
    substring(b.[text], (statement_start_offset/2) + 1, ((case statement_end_offset when -1 then datalength(b.[text]) else statement_end_offset end - statement_start_offset)/2) + 1) as query_text,
    c.query_plan,
    a.*
from sys.dm_exec_requests a
outer apply sys.dm_exec_sql_text(a.[sql_handle]) b
outer apply sys.dm_exec_query_plan(a.plan_handle) c
where a.session_id <> @@spid
	and a.session_id > 50
	and a.[status] <> 'background' 
order by
    total_elapsed_time desc