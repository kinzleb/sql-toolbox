use glass4
go

set transaction isolation level read uncommitted

declare
	@PartyID int = -2147483282,
	@LoadDateStart datetime = '2015-07-30 13:00:00'

select
	b.DP_DataProviderName as DataProviderName,
	a.*
from (
	select
		LE_PartyID as PartyID,
		LE_ErrorProcessDate as ErrorProcessDate,
		LE_ErrorPackageName as ErrorPackageName,
		LE_ErrorSourceValue as ErrorSourceValue,
		LE_ErrorMessageText as ErrorMessageText,
		count(0) NumberOfErrors
	from Metric.LoadError
	where LE_PartyID = @PartyID
		and LE_ErrorDatetime >= @LoadDateStart
	group by
		LE_PartyID ,
		LE_ErrorProcessDate,
		LE_ErrorPackageName,
		LE_ErrorSourceValue,
		LE_ErrorMessageText
) a
inner join etl.DataProviderExt b
	on b.DP_DataProviderID = a.PartyID
order by
	a.PartyID,
	a.ErrorProcessDate desc


select
	LE_ErrorPackageName as ErrorPackageName,
	count(0) NumberOfErrors
from Metric.LoadError
where LE_PartyID = @PartyID
	and LE_ErrorDatetime >= @LoadDateStart
group by
	LE_ErrorPackageName
order by
	count(0) desc
