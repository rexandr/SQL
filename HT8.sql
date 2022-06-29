-- Stat DB is used

	USE Stat; 
	GO

/*

	CREATE PROCEDURE usp_GetErrorInfo  
	AS  
	SELECT  
		ERROR_NUMBER() AS ErrorNumber  
		,ERROR_SEVERITY() AS ErrorSeverity  
		,ERROR_STATE() AS ErrorState  
		,ERROR_PROCEDURE() AS ErrorProcedure  
		,ERROR_LINE() AS ErrorLine  
		,ERROR_MESSAGE() AS ErrorMessage;  
	GO  

*/

--Transaction with LEG altering and time data adding through temp table

	BEGIN TRANSACTION

	BEGIN TRY
		
		--TEMP table creation for calculations

		CREATE TABLE #TimesAvia(
		NEW_KEY int primary key CLUSTERED,
		ATD time,
		TOFF time,
		BLHR float,
		ABHR float,
		TDWN time,
		ATA time
		)
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), '#TimeAvia created';


		--TEMP table filling with calculations

		INSERT INTO #TimesAvia
			SELECT
				NEW_KEY,
				FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:00:00'), 'HH:mm') AS ATD,
				FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm') AS TOFF,
				CAST(BLHR AS float)*60 AS iBLHR,
				CAST(ABHR AS float)*60 AS iABHR,
				FORMAT(DATEADD(mi,CAST(ABHR AS float)*60,FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm:ss')), 'HH:mm') AS TDWN,
				FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT)+CAST(BLHR AS float)*60,'00:00:00'), 'HH:mm') AS ATA
			FROM 
				LEG 
			ORDER BY 
				NEW_KEY;
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in #TimeAvia';

		--Add new column for calculation

		ALTER TABLE 
			#TimesAvia
				ADD 
					ATA_TDWN time;
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'ATA_TDWN column added to #TimeAvia.';

		--Updating temp's new column with new extra calculations
		
		UPDATE #TimesAvia
			SET 
				ATA_TDWN = FORMAT(DATEADD(mi,DATEDIFF(mi, ATA, TDWN),'00:00:00'), 'HH:mm')
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in #TimeAvia ATA_TDWN column';

		--ALTERING LEG table with new time columns

		ALTER TABLE 
		LEG 
			ADD 
				ATD time,
				TOFF time,
				TDWN time,
				ATA time
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'LEG table was ALTERED with 4 new columns';

		--UPDATING LEG's new columns with time data from temptable

		UPDATE LEG
			SET 
				LEG.ATD = T.ATD,
				LEG.TOFF = 
					IIF(T.TDWN>T.ATA,
						FORMAT(DATEADD(mi,DATEDIFF(mi, T.ATA_TDWN, T.TOFF),'00:00:00'), 'HH:mm'),
						T.TOFF),
				LEG.TDWN = 
					IIF(
						T.TDWN>T.ATA,
						T.ATA,
						T.TDWN
					),
				LEG.ATA = T.ATA

			FROM #TimesAvia AS T
			WHERE LEG.NEW_KEY = T.NEW_KEY
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG. 4 new columns have been filled';

		--TEMPtable dropping

		DROP TABLE #TimesAvia;
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), '#TimeAvia has been dropped';

	IF @@TRANCOUNT>0 COMMIT TRANSACTION
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'Transaction commited';
	END TRY

				BEGIN CATCH
					SELECT 
						ERROR_NUMBER() AS ERROR_N,
						ERROR_MESSAGE() AS ERROR_M,
						ERROR_SEVERITY() AS ERROR_SEV,
						ERROR_LINE () AS ERROR_L,
						ERROR_PROCEDURE () AS ERROR_PROC,
						ERROR_STATE () AS ERROR_$TATE,
						@@TRANCOUNT AS COUNT_TRANS
				IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;
				INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'Transaction rollbacked';
				THROW;
				END CATCH 

	GO



	BEGIN TRANSACTION

		BEGIN TRY
		
			--TEMP table creation for calculations

			CREATE TABLE #TimesAvia(
			NEW_KEY int primary key CLUSTERED,
			ATD time,
			TOFF time,
			BLHR float,
			ABHR float,
			TDWN time,
			ATA time
			)

			--TEMP table filling with calculations

			INSERT INTO #TimesAvia
				SELECT
					NEW_KEY,
					FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:00:00'), 'HH:mm') AS ATD,
					FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm') AS TOFF,
					CAST(BLHR AS float)*60 AS iBLHR,
					CAST(ABHR AS float)*60 AS iABHR,
					FORMAT(DATEADD(mi,CAST(ABHR AS float)*60,FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm:ss')), 'HH:mm') AS TDWN,
					FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT)+CAST(BLHR AS float)*60,'00:00:00'), 'HH:mm') AS ATA
				FROM 
					LEG 
				ORDER BY 
					NEW_KEY;

			--Add new column for calculation

			ALTER TABLE 
				#TimesAvia
					ADD 
						ATA_TDWN time;
	
			--Updating temp's new column with new extra calculations
		
			UPDATE #TimesAvia
				SET 
					ATA_TDWN = FORMAT(DATEADD(mi,DATEDIFF(mi, ATA, TDWN),'00:00:00'), 'HH:mm') 

			--ALTERING LEG table with new time columns

			ALTER TABLE 
			LEG 
				ADD 
					ATD time,
					TOFF time,
					TDWN time,
					ATA time

			--UPDATING LEG's new columns with time data from temptable

			UPDATE LEG
				SET 
					LEG.ATD = T.ATD,
					LEG.TOFF = 
						IIF(T.TDWN>T.ATA,
							FORMAT(DATEADD(mi,DATEDIFF(mi, T.ATA_TDWN, T.TOFF),'00:00:00'), 'HH:mm'),
							T.TOFF),
					LEG.TDWN = 
						IIF(
							T.TDWN>T.ATA,
							T.ATA,
							T.TDWN
						),
					LEG.ATA = T.ATA

				FROM #TimesAvia AS T
				WHERE LEG.NEW_KEY = T.NEW_KEY

			--TEMPtable dropping
			--SELECT * FROM #TimesAvia;
			DROP TABLE #TimesAvia;

		IF @@TRANCOUNT>0 COMMIT TRANSACTION
		END TRY

					BEGIN CATCH
						SELECT 
							ERROR_NUMBER() AS ERROR_N,
							ERROR_MESSAGE() AS ERROR_M,
							ERROR_SEVERITY() AS ERROR_SEV,
							ERROR_LINE () AS ERROR_L,
							ERROR_PROCEDURE () AS ERROR_PROC,
							ERROR_STATE () AS ERROR_$TATE,
							@@TRANCOUNT AS COUNT_TRANS
					IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;
					THROW;
					END CATCH 
					GO


--SELECT * FROM LEG;

				-- convert integer to hh:mm
/*
	SELECT 
		SUBSTRING(FLTID, 4, 4) AS ATD, 
		CONCAT(SUBSTRING(FLTID, 4, 2), ':', SUBSTRING(FLTID, 6, 2)) AS ATD2,
		FORMAT(DATEADD(d,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:00:00'), 'HH:mm') AS EXPEREMENT,
		FORMAT(DATEADD(hh,CAST(SUBSTRING(FLTID, 4, 2) AS INT)%24,'00:00:00'), 'HH:mm') AS HHOURS,
		FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 6, 2) AS INT)%60,'00:00:00'), 'HH:mm') AS MMINUTES,
		FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:00:00'), 'HH:mm') AS ALLMINUTES,
		CAST(SUBSTRING(FLTID, 4, 2) AS INT)%24 AS DEV24,
		LEN(CAST(SUBSTRING(FLTID, 4, 2) AS INT)%24) AS LENN
	FROM 
		RawAvia 
	GROUP BY 
		FLTID 
	ORDER BY 
		FLTID;

		*/

/*
IIF(
			(LEN(CAST(SUBSTRING(FLTID, 4, 2) AS INT)%24))=1, 
			
			CONCAT('0', 
				CAST(
					CAST(
						SUBSTRING(FLTID, 4, 2) AS INT)%24) AS varchar
				) AS varchar
			)
			
			
			,
			CAST(SUBSTRING(FLTID, 4, 2) AS INT)%24
			) AS HHOUR,
*/
GO

--CREATING TEMP table
	CREATE TABLE #TimesAvia(
		NEW_KEY int primary key CLUSTERED,
		ATD time,
		TOFF time,
		BLHR float,
		ABHR float,
		TDWN time,
		ATA time
	)

	INSERT INTO #TimesAvia
		SELECT
			NEW_KEY,
			FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:00:00'), 'HH:mm') AS ATD,
			FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm') AS TOFF,
			CAST(BLHR AS float)*60 AS iBLHR,
			CAST(ABHR AS float)*60 AS iABHR,
			FORMAT(DATEADD(mi,CAST(ABHR AS float)*60,FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm:ss')), 'HH:mm') AS TDWN,
			FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT)+CAST(CAST(BLHR AS float)*60 AS INT),'00:00:00'), 'HH:mm') AS ATA
		FROM 
			LEG 
		ORDER BY 
			NEW_KEY;

	ALTER TABLE 
		#TimesAvia
			ADD 
				ATA_TDWN time;
	
	UPDATE #TimesAvia
		SET 
			ATA_TDWN = FORMAT(DATEADD(mi,DATEDIFF(mi, ATA, TDWN),'00:00:00'), 'HH:mm') 
	
	--SELECT * FROM #TimesAvia;

	--DROP TABLE #TimesAvia;

GO

--CREATING LEG_2 for experemental data
CREATE TABLE LEG_2
	(
		ID int PRIMARY KEY CLUSTERED,
		FLTID varchar(10) NOT NULL,
		DATOP date NOT NULL,
		LEGNO int NULL,
		DEPSTN int NOT NULL,
		ARRSTN int NOT NULL,
		CTRYCDFR int NOT NULL,
		CTRYCDTO int NOT NULL,
		BLHR float NOT NULL,
		ABHR float NOT NULL,
		GRPNO varchar(10) NOT NULL,
		STC_ID int NOT NULL,
		AIRCRAFT_ID int NOT NULL
	);

	ALTER TABLE 
		LEG_2 
			ADD 
				NEW_KEY int DEFAULT 0 NOT NULL;

	INSERT INTO LEG_2
		SELECT * FROM LEG;


	ALTER TABLE 
			LEG_2 
				ADD 
					ATD time,
					TOFF time,
					TDWN time,
					ATA time

	--SELECT * FROM LEG_2;
	--DROP TABLE LEG_2;
	GO

UPDATE LEG_2
	SET 
		LEG_2.ATD = T.ATD,
		LEG_2.TOFF = 
			IIF(T.TDWN>T.ATA,
				FORMAT(DATEADD(mi,DATEDIFF(mi, T.ATA_TDWN, T.TOFF),'00:00:00'), 'HH:mm'),
				T.TOFF),
		LEG_2.TDWN = 
			IIF(
				T.TDWN>T.ATA,
				T.ATA,
				T.TDWN
			),
		LEG_2.ATA = T.ATA

	FROM #TimesAvia AS T
	WHERE LEG_2.NEW_KEY = T.NEW_KEY


SELECT 
	NEW_KEY, 
	ATD, 
	TOFF, 
	TDWN, 
	ATA, 
	ATA_TDWN,
	DATEDIFF(mi, ATA_TDWN, TOFF) AS TOOF_ATA_TDWN, 
	DATEDIFF(mi, T.ATA, T.TDWN), 
	FORMAT(DATEADD(mi,DATEDIFF(mi, T.TOFF, DATEDIFF(mi, T.ATA, T.TDWN)),'00:00:00'), 'HH:mm'), 
	FORMAT(DATEADD(mi,DATEDIFF(mi, T.TOFF, FORMAT(DATEADD(mi,DATEDIFF(mi, T.ATA, T.TDWN),'00:00:00'), 'HH:mm') ),'00:00:00'), 'HH:mm'), 
	FORMAT(DATEADD(mi,DATEDIFF(mi, T.ATA, T.TDWN),'00:00:00'), 'HH:mm'), 
	FORMAT(DATEADD(mi,DATEDIFF(mi, ATA_TDWN, TOFF),'00:00:00'), 'HH:mm') 
FROM 
	#TimesAvia AS T

GO

SELECT * FROM COUNTRIES;
GO


	BEGIN TRANSACTION

		BEGIN TRY
		
		--ADD new column to LEG for scandinavian indication
			ALTER TABLE 
						COUNTRIES
							ADD
								SCANDINAVIAN int
		--INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'SCANDINAVIAN column added to LEG'
		
		
		--Updating column with scandinavian indication
		
			UPDATE 
				COUNTRIES
					SET 
						SCANDINAVIAN = 
							(CASE 
								WHEN ID = 29 THEN 1
								WHEN ID = 35 THEN 1
								WHEN ID = 12 THEN 1
								WHEN ID = 20 THEN 1
								WHEN ID = 8 THEN 1
								WHEN ID = 13 THEN 1
								ELSE 0
							END)		
			INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG. Scandinavian indication added';

			IF @@TRANCOUNT>0 COMMIT TRANSACTION
			INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'Transaction with scandinavian indication commited';
		END TRY

					BEGIN CATCH
						SELECT 
							ERROR_NUMBER() AS ERROR_N,
							ERROR_MESSAGE() AS ERROR_M,
							ERROR_SEVERITY() AS ERROR_SEV,
							ERROR_LINE () AS ERROR_L,
							ERROR_PROCEDURE () AS ERROR_PROC,
							ERROR_STATE () AS ERROR_$TATE,
							@@TRANCOUNT AS COUNT_TRANS
					IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;
					INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'Transaction with scandinavian indication rollbacked';
					THROW;
					END CATCH 
					GO

SELECT * FROM LOGS;
SELECT ACTYP FROM AIRCRAFT GROUP BY ACTYP;
SELECT * FROM COUNTRIES ORDER BY SCANDINAVIAN;

		ALTER TABLE 
					COUNTRIES
						DROP COLUMN
									SCANDINAVIAN;

	GO
	

	SELECT * FROM
		(SELECT 
			FLTID,
			BLHR,
			ATD,
			ATA,
			FORMAT(DATEADD(mi,DATEDIFF(mi, ATD, ATA),'00:00:00'), 'HH:mm') AS a,
			FORMAT(DATEADD(mi,CAST(CAST(BLHR AS float)*60 AS INT),'00:00:00'), 'HH:mm') AS b,
			CAST(BLHR AS float)*60 AS d
		FROM 
			LEG) AS c
	WHERE
		c.a <> c.b;


SELECT ATA, DATENAME(n,ATA) FROM LEG; SELECT ATA, DATENAME(hh,ATA) FROM LEG;--https://docs.microsoft.com/ru-ru/sql/t-sql/functions/datename-transact-sql?view=sql-server-ver16

SELECT ATA, DATEPART(hh,ATA) FROM LEG;

SELECT @@DATEFIRST;
SET DATEFIRST 1;
SELECT @@DATEFIRST;
SET DATEFIRST 7;

SELECT DATEPART(year, '12:10:30.123')  
    ,DATEPART(month, '12:10:30.123')  
    ,DATEPART(day, '12:10:30.123')  
    ,DATEPART(dayofyear, '12:10:30.123')  
    ,DATEPART(weekday, '12:10:30.123');  

SELECT DATEPART (tzoffset, '2007-05-10  00:00:01.1234567 +05:10');  

SELECT TIMEFROMPARTS ( 23, 59, 59, 0, 0 ) AS Result;  

UPDATE UnnamedTable SET MergeDT = DATEADD(hour,  [time] / 100,
                                  DATEADD(minute,[time] % 100,DATE_TAKEN))


SELECT

CONVERT(VARCHAR,[Column] / 60) + ':' + RIGHT('00' + CONVERT(VARCHAR,[Column] % 60),2)

FROM

[Table]


select cast((@time / 60) as varchar(2)) + ':' + cast((@time % 60) as varchar(2))

SELECT CONVERT(char(8), DATEADD(second, Duration, ''), 114) AS Duration ...

SELECT DATEDIFF (mi, S.NEW_ATD, S.ATD) AS DIFF
	FROM
		(SELECT
			NEW_KEY,
			FLTID,
			--FORMAT(DATEADD(hh,CAST(SUBSTRING(FLTID, 4, 4)/60 AS INT),DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4)%60 AS INT),'00:00:00')), 'HH:mm') AS NEW_ATD,
			FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:00:00'), 'HH:mm') AS ATD,
			FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm') AS TOFF,
			CAST(BLHR AS float)*60 AS iBLHR,
			FORMAT(DATEADD(mi,CAST(BLHR AS float)*60,'00:00:00'), 'HH:mm') AS IIBLHR,
			CAST(ABHR AS float)*60 AS iABHR,
			FORMAT(DATEADD(mi,CAST(ABHR AS float)*60,FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT),'00:20:00'), 'HH:mm:ss')), 'HH:mm') AS TDWN,
			FORMAT(DATEADD(mi,CAST(SUBSTRING(FLTID, 4, 4) AS INT)+CAST(BLHR AS float)*60,'00:00:00'), 'HH:mm') AS ATA

		FROM 
			LEG) AS S
			GO








		--ALTERING LEG table with new time columns
			IF (SELECT COL_LENGTH('LEG', 'ATA')) IS NULL
			BEGIN
				ALTER TABLE 
				LEG 
					DROP COLUMN 
						ATD,
						TOFF,
						TDWN,
						ATA
			END
			GO



		IF (SELECT COL_LENGTH('LEG', 'ATA')) IS NULL
			BEGIN
				ALTER TABLE 
				LEG 
					ADD 
						ATD datetime,
						TOFF datetime,
						TDWN datetime,
						ATA datetime
			END
			GO














			SELECT * FROM LEG;

WITH LEG_CTE (NEW_KEY, ATD, ATA, TOFF, TDWN, DIF_ATA_TDWN)
AS
(
		SELECT 
			NEW_KEY,
			CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60) AS ATD,
			CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT) AS ATA,
			(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20 AS TOFF,
			(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT) AS TDWN,
				((CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT))
				-
				(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT))
			 AS DIF_ATA_TDWN
		FROM
			LEG
)
UPDATE LEG
	SET 
		LEG.ATD = DATEADD(mi,LEG_CTE.ATD,(CAST(DATOP AS datetime))),
		LEG.TOFF = 
			DATEADD(mi,
				CASE 
					WHEN LEG_CTE.DIF_ATA_TDWN>0
					THEN LEG_CTE.TOFF - LEG_CTE.DIF_ATA_TDWN
					ELSE LEG_CTE.TOFF
				END
			,(CAST(DATOP AS datetime))),
		LEG.TDWN = 
			DATEADD(mi,
				CASE 
					WHEN LEG_CTE.DIF_ATA_TDWN>0
					THEN LEG_CTE.ATA
					ELSE LEG_CTE.TDWN
				END
		,(CAST(DATOP AS datetime))),
		LEG.ATA = DATEADD(mi,LEG_CTE.ATA,(CAST(DATOP AS datetime)))
	FROM
		LEG_CTE
		
	WHERE LEG.NEW_KEY = LEG_CTE.NEW_KEY

/*
	SELECT * FROM
		(SELECT 
			FLTID,
			BLHR,
			ATD,
			ATA,
			FORMAT(DATEADD(mi,DATEDIFF(mi, ATD, ATA),'00:00:00'), 'HH:mm') AS a,
			FORMAT(DATEADD(mi,CAST(CAST(BLHR AS float)*60 AS INT),'00:00:00'), 'HH:mm') AS b,
			CAST(BLHR AS float)*60 AS d
		FROM 
			LEG) AS c
	WHERE
		c.a <> c.b;
*/
GO

















	SELECT 1
			FROM   INFORMATION_SCHEMA.COLUMNS
			WHERE  TABLE_NAME = 'LEG'
					AND COLUMN_NAME = 'ATA'
					AND TABLE_SCHEMA = 'DBO'

	SELECT COL_LENGTH('LEG', 'ATA')


	ALTER DATABASE CURRENT SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT=ON;
	GO


	--ADDITIONAL INDEXES 

	SELECT * FROM sys.indexes WHERE name = 'IX_LEG_COUNTRYFROM';
	SELECT * FROM sys.indexes WHERE name = 'IX_AIRCRAFT_TYPE';

	IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_LEG_COUNTRYFROM' AND object_id = OBJECT_ID('LEG'))
    BEGIN
        CREATE NONCLUSTERED INDEX IX_LEG_COUNTRYFROM
		ON LEG (CTRYCDFR);
    END

	IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_LEG_COUNTRYTO' AND object_id = OBJECT_ID('LEG'))
    BEGIN
        CREATE NONCLUSTERED INDEX IX_LEG_COUNTRYTO
		ON LEG (CTRYCDTO);
    END

	IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_AIRCRAFT_TYPE' AND object_id = OBJECT_ID('AIRCRAFT'))
    BEGIN
        CREATE NONCLUSTERED INDEX IX_AIRCRAFT_TYPE
		ON AIRCRAFT (ACTYP);
    END

	GO



	--ADD new column to COUNTRIES table if not exists

	IF (SELECT COL_LENGTH('COUNTRIES', 'SCANDINAVIAN')) IS NULL
			BEGIN
				ALTER TABLE 
						COUNTRIES
							ADD
								SCANDINAVIAN int
			END


			GO


	SELECT FLTID, DATOP, DEPSTN, ARRSTN FROM LEG ORDER BY DATOP, FLTID, DEPSTN, ARRSTN;
	go


	--IN Memory

	USE master;
	GO

	CREATE DATABASE Stat_InM
	GO

	ALTER DATABASE Stat_InM SET AUTO_CLOSE OFF;
	GO

	ALTER DATABASE Stat_InM ADD FILEGROUP stat_inm_group
		CONTAINS MEMORY_OPTIMIZED_DATA;

	ALTER DATABASE Stat_InM ADD FILE (name='stat_inm_file', filename = 'd:\SIGMA\DB\L8\stat_inm_file')
		TO FILEGROUP stat_inm_group;
	GO

	USE Stat_InM;
	GO

	CREATE TABLE INM_RawAvia
		(
			FLTID varchar(10) NOT NULL,
			DATOP varchar(30) NOT NULL,
			LEGNO varchar(10) NULL,
			DEPSTN varchar(3) NOT NULL,
			ARRSTN varchar(3) NOT NULL,
			CTRYCDFR varchar(2) NOT NULL,
			CTRYCDTO varchar(2) NOT NULL,
			BLHR varchar(10) NOT NULL,
			ABHR varchar(10) NOT NULL,
			GRPNO varchar(10) NOT NULL,
			STC varchar(1) NOT NULL,
			ACREG varchar(10) NOT NULL,
			ACVER varchar(10) NOT NULL,
			ACOWN varchar(3) NOT NULL,
			ACTYP varchar(3) NOT NULL,
			ACTYP_NAME varchar(50) NOT NULL,
			CLASS varchar(1) NOT NULL,
			CONFIGA varchar(10) NULL,
			SEATED varchar(10) NULL,
			INDEX IX_LEG_NoCl NONCLUSTERED (FLTID, DATOP, DEPSTN, ARRSTN)
		)
	WITH
		(
		MEMORY_OPTIMIZED=ON,
		DURABILITY=SCHEMA_ONLY
		);

	BULK INSERT Stat_InM.dbo.INM_RawAvia
		FROM 'd:\SIGMA\DB\Source1Avia_26845.csv'
		WITH (
			FIRSTROW=2,
			FIELDQUOTE = '"',
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '0x0a');
	GO


	--STATIONS

	CREATE TABLE INM_STATIONS
		(
			ID int IDENTITY(1,1) PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT=200),
			STATION varchar(30) NOT NULL
		)
	WITH
		(
		MEMORY_OPTIMIZED=ON,
		DURABILITY=SCHEMA_AND_DATA
		);

	INSERT INTO 
		INM_STATIONS(STATION)
			SELECT 
				DEPSTN 
			FROM 
				INM_RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						INM_STATIONS 
					WHERE 
						INM_RawAvia.DEPSTN=INM_STATIONS.STATION 
					GROUP BY STATION) 
			GROUP BY 
				DEPSTN
			UNION
			SELECT 
				ARRSTN 
			FROM 
				INM_RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						INM_STATIONS 
					WHERE 
						INM_RawAvia.ARRSTN=INM_STATIONS.STATION 
					GROUP BY STATION) 
			GROUP BY 
				ARRSTN;

GO



	--COUNTRIES 

	CREATE TABLE INM_COUNTRIES
		(
			ID int IDENTITY(1,1) PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT=100),
			COUNTRY varchar(30) NOT NULL
		)
	WITH
		(
		MEMORY_OPTIMIZED=ON,
		DURABILITY=SCHEMA_AND_DATA
		);

	INSERT INTO 
		INM_COUNTRIES(COUNTRY)
			SELECT 
				CTRYCDFR 
			FROM 
				INM_RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						INM_COUNTRIES 
					WHERE 
						INM_RawAvia.CTRYCDFR=INM_COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY 
				CTRYCDFR
			UNION
			SELECT 
				CTRYCDTO 
			FROM 
				INM_RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						INM_COUNTRIES 
					WHERE 
						INM_RawAvia.CTRYCDTO=INM_COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY 
				CTRYCDTO;


	SELECT * FROM INM_RawAvia;
	SELECT * FROM INM_STATIONS;
	SELECT * FROM INM_COUNTRIES;



	SELECT * FROM fn_helpcollations () ;
	SELECT * FROM sys.objects ;
	SELECT * FROM sys.schemas;