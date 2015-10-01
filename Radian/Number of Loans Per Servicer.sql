use glass4
go

set transaction isolation level read uncommitted

--Loans Loaded to date
select
	b.[DP_DataProviderID] as DataProviderID,
    b.[DP_DataProviderName] as DataProviderName,
    count(0) as NumLoans,
	min(a.LNS_RowStartDate) as DataStartDate,
	max(a.LNS_RowChangeDate) as DataEndDate,
	c.InitialLoadDate,
	c.LatestLoadDate
from [DW].[Loans] a
inner join [ETL].[DataProviderExt] b
    on b.[DP_DataProviderID] = a.LNS_PartyID
left outer join (
	select
		LP_PartyID,
		min(LP_PopulationDateTime) as InitialLoadDate,
		max(LP_PopulationDateTime) as LatestLoadDate
	from [Metric].[LoadPopulation]
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
declare @PartyID int = -2147483376
select
	a.DataDate,
	isnull(a.LoansWithNewInfo,0) as LoansWithNewInfo,
	isnull(c.LoansBoarded,0) as LoansBoarded,
	isnull(b.LoansLastUpdated,0) as LoansLastUpdated
from (
	select
		a.LN_RowStartDate as DataDate,
		count(0) as LoansWithNewInfo
	from DW.Loan a
	inner join DW.Loans b
		on b.LNS_LoansID = a.LN_LoansID
	where b.LNS_PartyID = @PartyID
	group by
		a.LN_RowStartDate
) a
left outer join (
	select
		a.LNS_RowChangeDate,
		count(0) as LoansLastUpdated
	from [DW].[Loans] a
	where a.LNS_PartyID = @PartyID
	group by
		a.LNS_RowChangeDate
) b
	on b.LNS_RowChangeDate = a.DataDate
left outer join (
	select
		a.LNS_RowStartDate,
		count(0) as LoansBoarded
	from DW.Loans a
	where a.LNS_PartyID = @PartyID
	group by
		a.LNS_RowStartDate
) c
	on c.LNS_RowStartDate = a.DataDate
order by
	a.DataDate
option(recompile)


--Files not yet processed
--on IS server, see if LQE_ProcessAuditID is null, that means it hasn't completed processing
/*
select *
from AdminUtils.[dbo].[Glass4LoadQueue]
where LQE_DatabaseName = 'ServicerRaw_Ocwen'
order by LQE_ProcessDate desc
*/