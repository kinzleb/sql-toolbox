use SSISDB
GO

execute as login = 'GFREVIEW\svcSQLAgent'
	
	Declare @execution_id bigint;
	DECLARE @LoggingLevel smallint = 2 --logging level
	declare @package nvarchar(100)=N'Controller.dtsx'
	declare @folder nvarchar(100)='GLASS4'
	declare @server nvarchar(255);
	declare @db nvarchar(255);
	declare @processDate datetime;
	declare @queueID int;
	declare @project nvarchar(100)='GLASS4 Load-Beta';
	declare @environment int=2;

	select @environment=reference_id
	from internal.environment_references
	where environment_name='GLASS4_Beta'

	select
		@server='DAL-D2S-SQL',
		@db='ServicerRaw_Ocwen',
		@processDate='2015-01-26',
		@queueID=-2147482867


	if (@queueID is not null)
	begin


		--Make sure our package isnt already running, allow the previous to finish
		if (select count(*)
			from ssisdb.internal.execution_info
			where package_name=@package
				--and status in (2,5,8)
				and status in (2,5)
				and reference_id=@environment
			)=0 

		begin
			EXEC [SSISDB].[catalog].[create_execution] @package_name=@package, @execution_id=@execution_id OUTPUT, 
				@folder_name=@folder, 
				@project_name=@project, 
				@use32bitruntime=False, 
				@reference_id=@environment

		
			EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,  @object_type=50, @parameter_name=N'LOGGING_LEVEL', @parameter_value=@LoggingLevel
			EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,  @object_type=50, @parameter_name=N'SYNCHRONIZED', @parameter_value=1
			EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,  @object_type=20, @parameter_name=N'ProcessServer', @parameter_value=@server
			EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,  @object_type=20, @parameter_name=N'ProcessDB', @parameter_value=@db
			EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,  @object_type=20, @parameter_name=N'ProcessDate', @parameter_value=@processDate
			EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,  @object_type=30, @parameter_name=N'QueueID', @parameter_value=@queueID
			EXEC [SSISDB].[catalog].[start_execution] @execution_id

			select @execution_id

		end

	end


	revert
	go
