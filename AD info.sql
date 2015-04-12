-- Collect AD Groups for given login
EXECUTE AS LOGIN = 'gfreview\bkinzle'
SELECT * from sys.login_token
WHERE TYPE = 'WINDOWS GROUP'
REVERT


EXEC master.dbo.xp_logininfo'GFREVIEW\IT BI Developers','members'