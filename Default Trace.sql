--show alter, create, and drop operations that are still in the default trace files
declare @d1 datetime; 
declare @diff int; 
declare @curr_tracefilename varchar(500);  
declare @base_tracefilename varchar(500);  
declare @indx int ; 
declare @temp_trace table (
        obj_name nvarchar(256)
,       obj_id int
,       database_name nvarchar(256)
,       start_time datetime
,       event_class int
,       event_subclass int
,       object_type int
,       server_name nvarchar(256)
,       login_name nvarchar(256)
,       user_name nvarchar(256)
,       application_name nvarchar(256)
,       ddl_operation nvarchar(40) 
);
        
select @curr_tracefilename = path from sys.traces where is_default = 1 ;  
set @curr_tracefilename = reverse(@curr_tracefilename) 
select @indx  = PATINDEX('%\%', @curr_tracefilename) 
set @curr_tracefilename = reverse(@curr_tracefilename) 
set @base_tracefilename = LEFT( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc'; 
        
insert into @temp_trace 
select ObjectName
,       ObjectID
,       DatabaseName
,       StartTime
,       EventClass
,       EventSubClass
,       ObjectType
,       ServerName
,       LoginName
,       NTUserName
,       ApplicationName
,       'temp'
from ::fn_trace_gettable( @base_tracefilename, default )  
where EventClass in (46,47,164)
	and EventSubclass = 0
	and DatabaseID <> db_id('tempdb') ; 

update @temp_trace set ddl_operation = 'CREATE' where event_class = 46;
update @temp_trace set ddl_operation = 'DROP' where event_class = 47;
update @temp_trace set ddl_operation = 'ALTER' where event_class = 164; 
        
select
		obj_name
,       ddl_operation
,       obj_id
,       database_name
,       start_time
,       object_type
,       server_name
,       login_name
,       [user_name]
,       application_name
from @temp_trace
where object_type not in (21587) --don't bother with auto-statistics as it generates too much noise
	--and user_name not in ('svcSqlAgent', 'svcContinuousBuild')
order by start_time desc;