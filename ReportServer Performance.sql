use ReportServer
go

set transaction isolation level read uncommitted

select
	a.ItemPath,
	b.DataSource,
	a.DataSet,
	count(0) as NumberOfExecutions,
	convert(decimal(9,2), (avg(a.DataSetRetrieval) / 1000.) / 60.) as AvgDataSetRetrievalMinutes,
	format(avg(a.DataSetRowCount), '#,###') as AvgRowCount,
	convert(decimal(9,2), (avg(a.TimeDataRetrieval) / 1000.) / 60.) as Total_AvgTimeDataRetrievalMinutes,
	format(avg(a.ByteCount), '#,###') as Total_AvgByteCount,
	format(avg(a.[RowCount]), '#,###') as Total_AvgRowCount
from (
	select
		a.ItemPath,
		a.TimeDataRetrieval,
		a.ByteCount,
		a.[RowCount],
		b.DataSet.value('./Name[1]', 'varchar(max)') as DataSet,
		b.DataSet.value('./TotalTimeDataRetrieval[1]', 'int') as DataSetRetrieval,
		b.DataSet.value('./RowsRead[1]', 'int') as DataSetRowCount
	from [dbo].[ExecutionLog3] a
	cross apply AdditionalInfo.nodes('/AdditionalInfo/Connections/Connection/DataSets/DataSet') b(DataSet)
	where a.[Source] <> 'Cache'
) a
inner join (
	select
		a.[Path],
		c.[Path] as DataSource
	from dbo.[Catalog] a
	inner join dbo.DataSource b
		on b.ItemID = a.ItemID
	inner join dbo.[Catalog] c
		on c.ItemID = b.link
	where a.[Type] in (2,6)
) b
	on b.[Path] = a.ItemPath
where b.DataSource like '%DataTier2'
group by
	a.ItemPath,
	b.DataSource,
	a.DataSet
order by 
	Total_AvgTimeDataRetrievalMinutes desc,
	AvgDataSetRetrievalMinutes desc
