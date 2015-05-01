use ssisdb
go

set transaction isolation level read uncommitted

--SET THIS!
declare @servicer varchar(300) = 'ServicerRaw_%'

--executions rolled up
if object_id('tempdb..#results') is not null drop table #results
select
	a.servicer,
	datediff(mi, b.start_time, b.end_time) as duration_minutes,
	convert(date, b.[created_time]) as created_date,
	b.[created_time],
	c.statusText
into #results
from (
	select distinct
		execution_id,
		convert(varchar(300), parameter_value) as servicer
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
option(recompile)
select
	a.servicer,
	a.created_date,
	a.executions,
	a.avg_duration_minutes,
	stuff(b.statusList.value('.', 'varchar(8000)'), 1, 2, '') as statusList
from (
	select
		a.servicer,
		a.created_date,
		count(0) as executions,
		avg(a.duration_minutes) as avg_duration_minutes
	from #results a
	group by
		a.servicer,
		a.created_date
) a
cross apply (
	select
		[text()] = ', ' + a1.statusText + ' (' + convert(varchar, row_number() over (order by a1.[created_time])) + ' in ' + isnull(convert(varchar, a1.duration_minutes),'NA') + ' min)'
	from #results a1
	where a1.servicer = a.servicer
		and a1.created_date = a.created_date
	order by a1.[created_time]
	for xml path(''), type
) b(statusList)
order by
	created_date desc


--execution summary showing process (data) date
select
	a.servicer,
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
		execution_id,
		convert(varchar(300), parameter_value) as servicer
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
option(recompile)