SET NOCOUNT ON;

DECLARE 
	@JsonData nvarchar(max) = '
		{
		  "BatchIndex": 1,
		  "BatchInstanceName": "3d9a3cb7-bb2d-453a-b1f2-9cc2249f4671-1",
		  "FileIndex": 1,
		  "OriginatingTransactionId": "6df005be-8015-4879-a8f8-fa1cb5b78cfe",
		  "S3KeyName": "cb847180-003b-411c-a6ac-b8e85668955a",
		  "WorkflowID": "3d9a3cb7-bb2d-453a-b1f2-9cc2249f4671"
		}',
	@RootTableName nvarchar(4000) = N'CreateBatchInfo',
	@Schema nvarchar(128) = N'dbo',
	@DefaultStringPadding smallint = 20;

DROP TABLE IF EXISTS ##parsedJson;
WITH jsonRoot AS (
	SELECT 
		0 as parentLevel, 
		CONVERT(nvarchar(4000),NULL) COLLATE Latin1_General_BIN2 as parentTableName, 
		0 AS [level], 
		[type] ,
		@RootTableName COLLATE Latin1_General_BIN2 AS TableName,
		[key] COLLATE Latin1_General_BIN2 as ColumnName,
		[value],
		ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS ColumnSequence
	FROM 
		OPENJSON(@JsonData, '$')
	UNION ALL
	SELECT 
		jsonRoot.[level] as parentLevel, 
		CONVERT(nvarchar(4000),jsonRoot.TableName) COLLATE Latin1_General_BIN2, 
		jsonRoot.[level]+1, 
		d.[type],
		CASE WHEN jsonRoot.[type] IN (4,5) THEN CONVERT(nvarchar(4000),jsonRoot.ColumnName) ELSE jsonRoot.TableName END COLLATE Latin1_General_BIN2,
		CASE WHEN jsonRoot.[type] IN (4) THEN jsonRoot.ColumnName ELSE d.[key] END,
		d.[value],
		ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS ColumnSequence
	FROM 
		jsonRoot
		CROSS APPLY OPENJSON(jsonRoot.[value], '$') d
	WHERE 
		jsonRoot.[type] IN (4,5) 
), IdRows AS (
	SELECT 
		-2 as parentLevel,
		null as parentTableName,
		-1 as [level],
		null as [type],
		TableName as Tablename,
		TableName+'Id' as columnName, 
		null as [value],
		0 as columnsequence
	FROM 
		(SELECT DISTINCT tablename FROM jsonRoot) j
), FKRows AS (
	SELECT 
		DISTINCT -1 as parentLevel,
		null as parentTableName,
		-1 as [level],
		null as [type],
		TableName as Tablename,
		parentTableName+'Id' as columnName, 
		null as [value],
		0 as columnsequence
	FROM 
		(SELECT DISTINCT tableName,parentTableName FROM jsonRoot) j
	WHERE 
		parentTableName is not null
)
SELECT 
	*,
	CASE [type]
		WHEN 1 THEN 
			CASE WHEN TRY_CONVERT(datetime2, [value], 127) IS NULL THEN 'nvarchar' ELSE 'datetime2' END
		WHEN 2 THEN 
			CASE WHEN TRY_CONVERT(int, [value]) IS NULL THEN 'float' ELSE 'int' END
		WHEN 3 THEN 
			'bit'
		END COLLATE Latin1_General_BIN2 AS DataType,
	round(CASE [type]
		WHEN 1 THEN 
			CASE WHEN TRY_CONVERT(datetime2, [value], 127) IS NULL THEN MAX(LEN([value])) OVER (PARTITION BY TableName, ColumnName) + @DefaultStringPadding ELSE NULL END
		WHEN 2 THEN 
			NULL
		WHEN 3 THEN 
			NULL
		END, -1) AS DataTypePrecision
INTO ##parsedJson
FROM jsonRoot
WHERE 
	[type] in (1,2,3)
UNION ALL SELECT IdRows.parentLevel, IdRows.parentTableName, IdRows.[level], IdRows.[type], IdRows.TableName, IdRows.ColumnName, IdRows.[value], -10 AS ColumnSequence, 'int IDENTITY(1,1) PRIMARY KEY' as datatype, null as datatypeprecision FROM IdRows 
UNION ALL SELECT FKRows.parentLevel, FKRows.parentTableName, FKRows.[level], FKRows.[type], FKRows.TableName, FKRows.ColumnName, FKRows.[value], -9 AS ColumnSequence, 'int' as datatype, null as datatypeprecision FROM FKRows 

-- For debugging:
-- SELECT * FROM ##parsedJson ORDER BY ParentLevel, level, tablename, columnsequence

DECLARE @CreateStatements nvarchar(max);

select
	@CreateStatements = COALESCE(@CreateStatements + CHAR(13) + CHAR(13), '') + 
	'CREATE TABLE ' + @Schema + '.' + TableName + CHAR(13) + '(' + CHAR(13) +
	stuff(b.col_def.value('.', 'varchar(max)'), 1, 2, '')
	+ CHAR(13)+')'
from (
	select distinct
		TableName
	from ##parsedJson
	where level = 0
) a
cross apply (
	select
		[text()] = ','+CHAR(13) + ColumnName + ' ' + DataType + ISNULL('('+CAST(DataTypePrecision AS nvarchar(20))+')','') +  CASE WHEN DataType like '%PRIMARY KEY%' THEN '' ELSE ' NULL' END
	FROM (
		SELECT DISTINCT 
			j.TableName, 
			j.ColumnName,
			MAX(j.ColumnSequence) AS ColumnSequence, 
			j.DataType, 
			j.DataTypePrecision, 
			j.[level] 
		FROM 
			##parsedJson j
			CROSS APPLY (SELECT TOP 1 ParentTableName + 'Id' AS ColumnName FROM ##parsedJson p WHERE j.TableName = p.TableName ) p
		GROUP BY
			j.TableName, j.ColumnName,p.ColumnName, j.DataType, j.DataTypePrecision, j.[level] 
	) j
	where j.TableName = a.TableName
		#and ColumnSequence > 0
	order by ColumnSequence
	for xml path(''), type
) b (col_def)


PRINT @CreateStatements;
