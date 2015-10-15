--determine if a column has been branded ascending
DBCC TRACEON (2388)
GO
DBCC SHOW_STATISTICS ('cenlar.CollectionCommunication', 'IXC_CollectionCommunication_RA_DataDate_RA_SourceKeyID')
GO