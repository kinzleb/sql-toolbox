use ssisdb
go

set transaction isolation level read uncommitted

declare @servicer varchar(300) = 'ServicerRaw_%'

--executions rolled up
;with cte as
(
	select
		datediff(mi, b.start_time, b.end_time) as duration_minutes,
		convert(date, b.[created_time]) as created_date,
		b.[created_time],
		c.statusText
	from (
		select distinct
			execution_id
		from [catalog].[execution_parameter_values]
		where parameter_data_type = 'String'
			and parameter_name = 'ProcessDB'
			and convert(varchar(300), parameter_value) like @servicer
	) a
	inner join [internal].[execution_info] b
		on b.execution_id = a.execution_id
	left outer join (
		values
			(1, 'created'),
			(2, 'running'),
			(3, 'canceled'),
			(4, 'failed'),
			(5, 'pending'),
			(6, 'ended unexpectedly'),
			(7, 'succeeded'),
			(8, 'stopping'),
			(9, 'completed')
	) c([status], statusText)
		on c.[status] = b.[status]
)
select
	a.created_date,
	a.executions,
	a.avg_duration_minutes,
	stuff(b.statusList.value('.', 'varchar(8000)'), 1, 2, '') as statusList
from (
	select
		a.created_date,
		count(0) as executions,
		avg(a.duration_minutes) as avg_duration_minutes
	from cte a
	group by
		a.created_date
) a
cross apply (
	select
		[text()] = ', ' + a1.statusText + ' (' + convert(varchar, row_number() over (order by a1.[created_time])) + ' in ' + isnull(convert(varchar, a1.duration_minutes),'NA') + ' min)'
	from cte a1
	where a1.created_date = a.created_date
	order by a1.[created_time]
	for xml path(''), type
) b(statusList)
order by
	created_date desc
option(recompile)

--execution summary showing process (data) date
select
	datediff(mi, b.start_time, b.end_time) as duration_minutes,
	c.ProcessDate,
	b.[created_time],
	b.[execution_id],
	b.[folder_name],
	b.[project_name],
	b.[package_name],
	case b.[status]
		when 1 then 'created'
		when 2 then 'running'
		when 3 then 'canceled'
		when 4 then 'failed'
		when 5 then 'pending'
		when 6 then 'ended unexpectedly'
		when 7 then 'succeeded'
		when 8 then 'stopping'
		when 9 then 'completed'
	end as statusText,
	b.[stopped_by_name],
	b.end_time
from (
	select distinct
		execution_id
	from [catalog].[execution_parameter_values]
	where parameter_data_type = 'String'
		and parameter_name = 'ProcessDB'
		and convert(varchar(300), parameter_value) like @servicer
) a
inner join [internal].[execution_info] b
	on b.execution_id = a.execution_id
cross apply (
	select top 1
		convert(datetime, parameter_value) as ProcessDate
	from [catalog].[execution_parameter_values] c1
	where c1.parameter_data_type = 'DateTime'
		and c1.parameter_name = 'ProcessDate'
		and c1.execution_id = a.execution_id
) c
order by
	b.[created_time] desc


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
	and operation_id = 356624                            --@parmaterize
order by
	message_time desc


--excution task details
select
	(execution_duration / 1000.) / 60. as execution_duration_minutes,
	start_time,
	end_time,
	case execution_result
		when 0 then 'Success'
		when 1 then 'Failure'
		when 2 then 'Completion'
		when 3 then 'Cancelled'
	end as execution_result,
	execution_path
from [catalog].[executable_statistics]
where execution_id = 404649                              --@parmaterize
order by
	execution_duration_minutes desc