select
	db_name(a.database_id) as db,
	d.login_name,
	convert(decimal(9,2),(a.total_elapsed_time / 1000.) / 60.) as elapsed_minutes,
    substring(b.[text], (statement_start_offset/2) + 1, ((case statement_end_offset when -1 then datalength(b.[text]) else statement_end_offset end - statement_start_offset)/2) + 1) as query_text,
    c.query_plan,
    a.*
from sys.dm_exec_requests a
outer apply sys.dm_exec_sql_text(a.[sql_handle]) b
outer apply sys.dm_exec_query_plan(a.plan_handle) c
inner join sys.dm_exec_sessions d
	on d.session_id = a.session_id
where a.session_id <> @@spid
	and a.session_id > 50
	and a.[status] <> 'background' 
order by
    total_elapsed_time desc