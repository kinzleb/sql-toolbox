set transaction isolation level read uncommitted;

select top 100
	db_name(b.dbid) as db,
	object_name(b.objectid, b.dbid) as objectname,
	substring(b.[text], (a.statement_start_offset/2) + 1, ((case a.statement_end_offset when -1 then datalength(b.[text]) else a.statement_end_offset end - a.statement_start_offset)/2) + 1) as query_text,
	c.query_plan,
	a.plan_generation_num,
	convert(decimal(9,2), ((a.total_elapsed_time/a.execution_count*1.) / 1000000.) / 60.) as avg_elapsed_time_minutes,
	a.creation_time,
	a.last_execution_time,
	a.execution_count,
	a.total_worker_time,
	a.last_worker_time,
	a.min_worker_time,
	a.max_worker_time,
	a.total_physical_reads,
	a.last_physical_reads,
	a.min_physical_reads,
	a.max_physical_reads,
	a.total_logical_writes,
	a.last_logical_writes,
	a.min_logical_writes,
	a.max_logical_writes,
	a.total_logical_reads,
	a.last_logical_reads,
	a.min_logical_reads,
	a.max_logical_reads,
	a.total_elapsed_time,
	a.last_elapsed_time,
	a.min_elapsed_time,
	a.max_elapsed_time,
	a.total_rows,
	a.last_rows,
	a.min_rows,
	a.max_rows
from sys.dm_exec_query_stats a
cross apply sys.dm_exec_sql_text(a.[sql_handle]) b
outer apply sys.dm_exec_query_plan(a.plan_handle) c
order by
	a.last_elapsed_time desc