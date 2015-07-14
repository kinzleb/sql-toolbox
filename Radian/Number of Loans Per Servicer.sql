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


--Loans Boarded by Data Date
select
	a.LNS_RowStartDate as DataDate,
	count(0) as LoansBoarded,
	max(b.LoansLastUpdated) as LoansLastUpdated
from [DW].[Loans] a (nolock)
left outer join (
	select
		a.LNS_RowChangeDate,
		count(0) as LoansLastUpdated
	from [DW].[Loans] a (nolock)
	group by
		a.LNS_RowChangeDate
) b
	on b.LNS_RowChangeDate = a.LNS_RowStartDate
where a.LNS_PartyID = -2147483646
group by
	a.LNS_RowStartDate,
	year(a.LNS_RowStartDate),
	month(a.LNS_RowStartDate)
order by
	a.LNS_RowStartDate


--Files not yet processed
--on IS server, see if LQE_ProcessAuditID is null, that means it hasn't completed processing
/*
select *
from AdminUtils.[dbo].[Glass4LoadQueue]
where LQE_DatabaseName = 'ServicerRaw_Ocwen'
order by LQE_ProcessDate desc
*/