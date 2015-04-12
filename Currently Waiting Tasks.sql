SELECT
    [owt].[session_id],
    [es].[host_name],
    [es].[login_name],
    [owt].[exec_context_id],
    [owt].[wait_duration_ms],
    [owt].[wait_type],
    [owt].[blocking_session_id],
    [owt].[resource_description],
    CASE [owt].[wait_type]
        WHEN N'CXPACKET' THEN
            RIGHT ([owt].[resource_description],
            CHARINDEX (N'=', REVERSE ([owt].[resource_description])) - 1)
        ELSE NULL
    END AS [Node ID],
    [es].[program_name],
    [est].[text],
    db_name([er].[database_id]) as database_name,
    [eqp].[query_plan],
    [er].[total_elapsed_time],
    [er].[cpu_time],
    [er].[logical_reads],
    [er].[reads],
    [er].[writes]
FROM sys.dm_os_waiting_tasks [owt]
INNER JOIN sys.dm_exec_sessions [es] ON
    [owt].[session_id] = [es].[session_id]
INNER JOIN sys.dm_exec_requests [er] ON
    [es].[session_id] = [er].[session_id]
OUTER APPLY sys.dm_exec_sql_text ([er].[sql_handle]) [est]
OUTER APPLY sys.dm_exec_query_plan ([er].[plan_handle]) [eqp]
WHERE
    [es].[is_user_process] = 1
ORDER BY
    [er].[total_elapsed_time] desc
    --[owt].[session_id],
    --[owt].[exec_context_id];
GO