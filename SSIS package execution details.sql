use ssisdb
go

set transaction isolation level read uncommitted

--SET THIS!
declare @execution_id int = 588271

--excution task details of leaf level tasks only
if object_id('tempdb..#results') is not null drop table #results
create table #results (
	execution_path varchar(900) not null primary key clustered,
	execution_duration bigint,
	start_time datetime,
	end_time datetime,
	execution_result int
)

insert #results
select
	isnull(stuff(a.execution_path, charindex('[', a.execution_path), charindex(']', a.execution_path) - charindex('[', a.execution_path), ''), a.execution_path) as execution_path,
	sum(a.execution_duration) as execution_duration,
	min(a.start_time) as start_time,
	max(a.end_time) as end_time,
	max(a.execution_result) as execution_result
from [catalog].[executable_statistics] a
where a.execution_id = @execution_id
group by
	isnull(stuff(a.execution_path, charindex('[', a.execution_path), charindex(']', a.execution_path) - charindex('[', a.execution_path), ''), a.execution_path)
option(recompile)

select
	a.execution_path,
	(a.execution_duration / 1000.) / 60. as execution_duration_minutes,
	a.start_time,
	a.end_time,
	case a.execution_result
		when 0 then 'Success'
		when 1 then 'Failure'
		when 2 then 'Completion'
		when 3 then 'Cancelled'
	end as execution_result
from #results a
where not exists (
		select *
		from #results a1
		where a1.execution_path <> a.execution_path
			and a1.execution_path like a.execution_path + '%'
	)
order by
	execution_duration_minutes desc


--execution message detail
select
	[message],
	[package_name],
	[event_name],
	case [message_type]
		when -1 then 'Unknown'
		when 120 then 'Error'
		when 110 then 'Warning'
		when 70 then 'Information'
		when 10 then 'Pre-validate'
		when 20 then 'Post-validate'
		when 30 then 'Pre-execute'
		when 40 then 'Post-execute'
		when 60 then 'Progress'
		when 50 then 'StatusChange'
		when 100 then 'QueryCancel'
		when 130 then 'TaskFailed'
		when 90 then 'Diagnostic'
		when 200 then 'Custom'
		when 140 then 'DiagnosticEx'
		when 400 then 'NonDiagnostic'
		when 80 then 'VariableValueChanged'
	end as message_type,
	case [message_source_type]
		when 10 then 'Entry APIs, such as T-SQL and CLR Stored procedures'
		when 20 then 'External process used to run package'
		when 30 then 'Package-level objects'
		when 40 then 'Control Flow tasks'
		when 50 then 'Control Flow containers'
		when 60 then 'Data Flow task'
	end as message_source_type,
	[message_source_name],
	[message_time],
	[subcomponent_name],
	[execution_path]
from [catalog].[event_messages]
where event_name not like '%validate%'
	and event_name like '%error%'
	and operation_id = @execution_id
order by
	message_time desc
option(recompile)