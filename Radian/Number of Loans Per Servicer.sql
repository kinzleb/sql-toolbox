use glass4
go

--Loans Loaded to date
select
	b.[DP_DataProviderID] as DataProviderID,
    b.[DP_DataProviderName] as DataProviderName,
    count(0) as NumLoans,
	min(a.LNS_RowStartDate) as DataStartDate,
	max(a.LNS_RowChangeDate) as DataEndDate,
	c.InitialLoadDate,
	c.LatestLoadDate
from [DW].[Loans] (nolock) a
inner join [ETL].[DataProviderExt] (nolock)  b
    on b.[DP_DataProviderID] = a.LNS_PartyID
left outer join (
	select
		LP_PartyID,
		min(LP_PopulationDateTime) as InitialLoadDate,
		max(LP_PopulationDateTime) as LatestLoadDate
	from [Metric].[LoadPopulation] (nolock) 
	group by
		LP_PartyID
) c
	on c.LP_PartyID = b.DP_DataProviderID
group by
	b.[DP_DataProviderID],
    b.[DP_DataProviderName],
	c.InitialLoadDate,
	c.LatestLoadDate
order by
	c.LatestLoadDate desc


--Loans Boarded by Month
select
	year(LNS_RowStartDate) as Year,
	month(LNS_RowStartDate) as Month,
	count(0) as LoansBoarded
from [DW].[Loans] (nolock)
where LNS_PartyID = -2147483646
group by
	year(LNS_RowStartDate),
	month(LNS_RowStartDate)
order by
	year(LNS_RowStartDate),
	month(LNS_RowStartDate)


--Files not yet processed
--on IS server, see if LQE_ProcessAuditID is null, that means it hasn't completed processing
/*
select *
from AdminUtils.[dbo].[Glass4LoadQueue]
where LQE_DatabaseName = 'ServicerRaw_Ocwen'
order by LQE_ProcessDate desc
*/