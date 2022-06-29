	
	USE master; 
	GO
	
	USE Stat; 
	GO


	SELECT * FROM LEG_CLASS;
	SELECT * FROM COUNTRIES;
	SELECT * FROM STC;
	SELECT * FROM STATIONS;
	SELECT * FROM AIRCRAFTS;
	SELECT * FROM LOGS;
	SELECT * FROM LEG_PAXCAT ORDER BY LEG_KEY;

	
	
	

	/*


	ROLLBACK TRANSACTION DBChanging;
	
	--SELECT ALL CONSTRAINTS 

	SELECT 
		TABLE_NAME,
		CONSTRAINT_TYPE,
		CONSTRAINT_NAME
	FROM 
		INFORMATION_SCHEMA.TABLE_CONSTRAINTS
	WHERE 
		TABLE_NAME='LEG_CLASS'
		OR
		TABLE_NAME='LEG'
		OR
		TABLE_NAME='LEG_PAXCAT'
		OR
		TABLE_NAME='AIRCRAFTS'
		OR
		TABLE_NAME='STC'
		OR
		TABLE_NAME='COUNTRIES'
		OR
		TABLE_NAME='STATIONS';

	
	ROLLBACK TRANSACTION IX_LEG;

	
	--SELECT ACTIVE TRANSACTIONS

				DBCC OPENTRAN;
				SELECT * FROM sys.sysprocesses WHERE open_tran = 1

				SELECT
				trans.session_id AS [SESSION ID],
				ESes.host_name AS [HOST NAME],login_name AS [Login NAME],
				trans.transaction_id AS [TRANSACTION ID],
				tas.name AS [TRANSACTION NAME],tas.transaction_begin_time AS [TRANSACTION 
				BEGIN TIME],
				tds.database_id AS [DATABASE ID],DBs.name AS [DATABASE NAME]
				FROM sys.dm_tran_active_transactions tas
				JOIN sys.dm_tran_session_transactions trans
				ON (trans.transaction_id=tas.transaction_id)
				LEFT OUTER JOIN sys.dm_tran_database_transactions tds
				ON (tas.transaction_id = tds.transaction_id )
				LEFT OUTER JOIN sys.databases AS DBs
				ON tds.database_id = DBs.database_id
				LEFT OUTER JOIN sys.dm_exec_sessions AS ESes
				ON trans.session_id = ESes.session_id
				WHERE ESes.session_id IS NOT NULL

				select * from sys.dm_tran_active_transactions 



	
		/*
		SELECT 
			TABLE_NAME,
			CONSTRAINT_TYPE,
			CONSTRAINT_NAME
		FROM 
			INFORMATION_SCHEMA.TABLE_CONSTRAINTS
		WHERE 
			TABLE_NAME='LEG';
		*/

		
		/*
						(CASE 
								WHEN DEPSTN = 29 OR ARRSTN = 29 THEN 29
								WHEN DEPSTN = 35 OR ARRSTN = 35 THEN 35
								WHEN DEPSTN = 12 OR ARRSTN = 12 THEN 12
								WHEN DEPSTN = 20 OR ARRSTN = 20 THEN 20
								WHEN DEPSTN = 8 OR ARRSTN = 8 THEN 8
								WHEN DEPSTN = 13 OR ARRSTN = 13 THEN 13
								ELSE DEPSTN
						END)
		*/

*/

	--SELECT ALL INDEXES 

	select i.[name] as index_name,
    substring(column_names, 1, len(column_names)-1) as [columns],
    case when i.[type] = 1 then 'Clustered index'
        when i.[type] = 2 then 'Nonclustered unique index'
        when i.[type] = 3 then 'XML index'
        when i.[type] = 4 then 'Spatial index'
        when i.[type] = 5 then 'Clustered columnstore index'
        when i.[type] = 6 then 'Nonclustered columnstore index'
        when i.[type] = 7 then 'Nonclustered hash index'
        end as index_type,
    case when i.is_unique = 1 then 'Unique'
        else 'Not unique' end as [unique],
    schema_name(t.schema_id) + '.' + t.[name] as table_view, 
    case when t.[type] = 'U' then 'Table'
        when t.[type] = 'V' then 'View'
        end as [object_type]
	from sys.objects t
		inner join sys.indexes i
			on t.object_id = i.object_id
		cross apply (select col.[name] + ', '
						from sys.index_columns ic
							inner join sys.columns col
								on ic.object_id = col.object_id
								and ic.column_id = col.column_id
						where ic.object_id = t.object_id
							and ic.index_id = i.index_id
								order by key_ordinal
								for xml path ('') ) D (column_names)
	where t.is_ms_shipped <> 1
	and index_id > 0
	order by i.[name]



	--SELECT UNMATCHING

	select CNTR_DEF from LEG
	WHERE CNTR_DEF NOT IN
	(SELECT COUNTRY_KEY from COUNTRIES)

	DROP INDEX IX_AIRCRAFT_TYPE ON AIRCRAFTS