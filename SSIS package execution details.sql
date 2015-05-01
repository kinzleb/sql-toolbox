use ssisdb
go

set transaction isolation level read uncommitted

--SET THIS!
declare @execution_id int = 674414

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
	and operation_id = @execution_id
order by
	message_time desc
option(recompile)


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
where execution_id = @execution_id
order by
	execution_duration_minutes desc
option(recompile)