declare @search_term varchar(max)
set @search_term = 'DAA'

select @search_term = 'use ? SET QUOTED_IDENTIFIER ON
select
    ''[''+db_name()+''].[''+c.name+''].[''+b.name+'']'' as [object],
    b.type_desc as [type],
    d.obj_def.value(''.'',''varchar(max)'') as [definition]
from (
    select distinct
        a.id
    from sys.syscomments a
    where a.[text] like ''%'+@search_term+'%''
) a
inner join sys.all_objects b
    on b.[object_id] = a.id
inner join sys.schemas c
    on c.[schema_id] = b.[schema_id]
cross apply (
    select
        [text()] = a1.[text]
    from sys.syscomments a1
    where a1.id = a.id
    order by a1.colid
    for xml path(''''), type
) d(obj_def)
where c.schema_id not in (3,4) -- avoid searching in sys and INFORMATION_SCHEMA schemas
    and db_id() not in (1,2,3,4) -- avoid sys databases'

if object_id('tempdb..#textsearch') is not null drop table #textsearch
create table #textsearch
(
    [object] varchar(300),
    [type] varchar(300),
    [definition] varchar(max)
)

insert #textsearch
exec sp_MSforeachdb @search_term

select *
from #textsearch
order by [object]