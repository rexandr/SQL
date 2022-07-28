
	--JOB SCHEDULING

	USE Master
		GO
		
		CREATE PROCEDURE usp_BulkInsert
		AS
		BEGIN
			SET NOCOUNT ON;

    -- The interval between cleanup attempts
		DECLARE @Run nvarchar(50)
		SET @Run = '03:00:00'

		WHILE 1 = 1
			BEGIN
				WAITFOR TIME @Run
				BEGIN
					/*
					INSERT INTO 
						Stat.dbo.IMPORT_LOG(LEG_KEY, INSERTED, UPDATED, U_I_STATUS)
					(SELECT 0, GETDATE(), GETDATE(), 'S');
					*/

					EXECUTE LEG_CURSOR;

					IF 
						 @Run < '24:00:00' AND @Run > '02:00:00'
							SET @Run = DATEADD(hh, 1, @Run)
					ELSE
						SET @Run = '03:00:00'
				END
			END
		END

		/*
		-- Run the procedure when the master database starts.
		EXEC sp_procoption    @ProcName = 'usp_BulkInsert',
						@OptionName = 'startup',
						@OptionValue = 'on'
		*/
	GO
	--USE Stat; 
	--GO
	--SELECT * FROM IMPORT_LOG;
	--DROP PROCEDURE usp_BulkInsert;
	
	*/
	GO
	
	--SECURITY GROUP

	CREATE PROCEDURE SEC_GROUP
	AS 
	
	SET NOCOUNT ON; 

		BEGIN

		--LOGINS

		CREATE LOGIN LEG_LOGIN  WITH PASSWORD = '8fdKJl3$nlNv3049jsKK';
		CREATE LOGIN ANOTHER_LOGIN  WITH PASSWORD = '8fdKJl3$nlNv3049jsKK';

		--USERS

		CREATE USER User_DB1 FOR LOGIN LEG_LOGIN;
		CREATE USER User_DB3 FOR LOGIN ANOTHER_LOGIN;

		--ROLE

		CREATE ROLE Developers;
		ALTER ROLE Developers
			ADD MEMBER User_DB1;
		ALTER ROLE Developers
			ADD MEMBER User_DB3;

		--GRANT

		DECLARE @OBJ nvarchar(200);  

		DECLARE GRANT_CURSOR CURSOR FOR   
		SELECT 
			name 
		FROM 
			sys.objects 
		WHERE 
			type IN ('U', 'PK', 'F', 'D', 'IF', 'V');  
  
		OPEN GRANT_CURSOR  
  
		FETCH NEXT FROM GRANT_CURSOR   
		INTO @OBJ 
  
		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			
			DECLARE @stm NVARCHAR(max)
			SET @stm = 'GRANT ALL ON'+@OBJ+'TO Developers';
			EXEC sp_executesql @stm
			
			FETCH NEXT FROM GRANT_CURSOR   
			INTO @OBJ 
		END   
		CLOSE GRANT_CURSOR;  
		DEALLOCATE GRANT_CURSOR;



		SELECT * FROM fn_builtin_permissions(default)
		select * from sys.objects WHERE type IN ('U', 'PK', 'F', 'D', 'IF', 'V')
		SELECT * FROM sysusers WHERE name like 'User%';
		SELECT name FROM sysusers WHERE name like 'User%';

		END
	GO

	--EXEC SEC_GROUP
--Folder scan


--CREATE PROCEDURE LEG_OUTPUT
	
	CREATE PROCEDURE LEG_START
	AS 
	
	SET NOCOUNT ON; 

		BEGIN

		--RECONFIG 

		-- this turns on advanced options and is needed to configure xp_cmdshell
		EXEC sp_configure 'show advanced options', '1'
		RECONFIGURE
		-- this enables xp_cmdshell
		EXEC sp_configure 'xp_cmdshell', '1' 
		RECONFIGURE

		--Table with files' list for cursor
	
		IF  NOT EXISTS (SELECT * FROM sys.objects 
		WHERE object_id = OBJECT_ID('LEG_OUTPUT') AND type in (N'U'))
		BEGIN
		CREATE TABLE 
			LEG_OUTPUT
				(
				ID int identity(1,1), 
				OUTPUT nvarchar(255) null
				);
		END

		--folder scan

		INSERT
			LEG_OUTPUT
				(OUTPUT) 
		EXEC 
			xp_cmdshell 
				'dir c:\Test_ETL\*.csv';

		--Imported tables' storing
	
		IF  NOT EXISTS (SELECT * FROM sys.objects 
		WHERE object_id = OBJECT_ID('IMPORTED') AND type in (N'U'))
		BEGIN
		CREATE TABLE 
			IMPORTED
				(
				ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
				IM_FILE_NAME varchar(50),
				IM_DATE date DEFAULT GETDATE()
				);
		--DROP TABLE IMPORTED;
		END

		--IMPORT LOG

		IF  NOT EXISTS (SELECT * FROM sys.objects 
		WHERE object_id = OBJECT_ID('IMPORT_LOG') AND type in (N'U'))
		BEGIN
		CREATE TABLE IMPORT_LOG
			(
				ID int IDENTITY(1,1) NOT NULL,
				LEG_KEY int NOT NULL,
				INSERTED date,
				UPDATED date,
				U_I_STATUS varchar(1) 
			)
		END
		--DROP TABLE IMPORT_LOG;


		--ARCHIVE CREATION
	
		IF  NOT EXISTS (SELECT * FROM sys.objects 
		WHERE object_id = OBJECT_ID('LEG_AR') AND type in (N'U'))
		BEGIN
		CREATE TABLE LEG_AR
				(
					AC_VER_TYP_KEY int NOT NULL,
					LEG_KEY int PRIMARY KEY CLUSTERED,
					FLTID varchar(10) NOT NULL,
					DATOP date NOT NULL,
					LEGNO int NULL,
					DEPSTN int NOT NULL,
					ARRSTN int NOT NULL,
					ATD datetime NOT NULL,
					ATA datetime NOT NULL,
					BLHR float NOT NULL,
					ABHR float NOT NULL,
					GRPNO varchar(10) NOT NULL,
					STC int NOT NULL,
					TOFF datetime NOT NULL,
					TDWN datetime NOT NULL,
					CNTR_DEF int NOT NULL,
					_CREATED datetime NOT NULL,
					_CHANGED datetime NOT NULL,
				);
		END

	
		IF  NOT EXISTS (SELECT * FROM sys.objects 
		WHERE object_id = OBJECT_ID('LEG_CLASS_AR') AND type in (N'U'))
		BEGIN
		CREATE TABLE LEG_CLASS_AR
		(
			LEG_KEY int NOT NULL,
			CLASS varchar(1) NOT NULL,
			CONFIGA int NOT NULL,
			SEATED int NOT NULL,
			_CREATED datetime NOT NULL,
			_CHANGED datetime NOT NULL,
		);
		END


		IF  NOT EXISTS (SELECT * FROM sys.objects 
		WHERE object_id = OBJECT_ID('LEG_PAXCAT_AR') AND type in (N'U'))
		BEGIN
		CREATE TABLE LEG_PAXCAT_AR
			(
				LEG_KEY int NOT NULL,
				PAXCAT varchar(5) NOT NULL,
				ACTBD int NOT NULL,
				ACTTFR int NOT NULL,
				ACTTRT int NOT NULL,
				_CREATED datetime NOT NULL,
				_CHANGED datetime NOT NULL,
			);

		--PRIMARY KEYS CLASS AND PAXCAT

		ALTER TABLE LEG_CLASS_AR
		ADD CONSTRAINT PK_LEG_AR_CLASS
		PRIMARY KEY(LEG_KEY, CLASS);

		ALTER TABLE LEG_PAXCAT_AR
		ADD CONSTRAINT PK_LEG_AR_PAXCAT
		PRIMARY KEY(LEG_KEY, PAXCAT);

		--FOREIGN KEYS

		ALTER TABLE LEG_AR
		ADD CONSTRAINT FK_LEG_AR_AIRCRAFT
		FOREIGN KEY (AC_VER_TYP_KEY) REFERENCES AIRCRAFTS(AC_VER_TYP_KEY);

		ALTER TABLE LEG_AR
		ADD CONSTRAINT FK_LEG_AR_STC
		FOREIGN KEY (STC) REFERENCES STC(STC_KEY);

		ALTER TABLE LEG_AR
		ADD CONSTRAINT FK_LEG_AR_CNTR
		FOREIGN KEY (CNTR_DEF) REFERENCES COUNTRIES(COUNTRY_KEY);

		ALTER TABLE LEG_AR
		ADD CONSTRAINT FK_LEG_AR_DEPSTN
		FOREIGN KEY (DEPSTN) REFERENCES STATIONS(STN_KEY);

		ALTER TABLE LEG_AR
		ADD CONSTRAINT FK_LEG_AR_ARRSTN
		FOREIGN KEY (ARRSTN) REFERENCES STATIONS(STN_KEY);
				
		ALTER TABLE LEG_CLASS_AR
		ADD CONSTRAINT FK_LEG_CLASS_AR_LEG
		FOREIGN KEY (LEG_KEY) REFERENCES LEG_AR(LEG_KEY);
			
		ALTER TABLE LEG_PAXCAT_AR
		ADD CONSTRAINT FK_LEG_AR_PAXCAT
		FOREIGN KEY (LEG_KEY) REFERENCES LEG_AR(LEG_KEY);

		--INDEXES

		CREATE NONCLUSTERED INDEX IX_LEG_AR_DEPSTN
		ON LEG_AR (DEPSTN); 

		CREATE NONCLUSTERED INDEX IX_LEG_AR_ARRSTN
		ON LEG_AR (ARRSTN); 

		CREATE NONCLUSTERED INDEX IX_LEG_AR_STC
		ON LEG_AR (STC);

		CREATE NONCLUSTERED INDEX IX_LEG_AR_AIRCRAFT
		ON LEG_AR (AC_VER_TYP_KEY);

		CREATE NONCLUSTERED INDEX IX_LEG_AR_CNTR
				ON LEG_AR (CNTR_DEF);
	END


GO

--CURSOR

	CREATE PROCEDURE LEG_CURSOR
	AS 
	
	SET NOCOUNT ON; 
		DECLARE @IM_FILE_NAME nvarchar(50);  

		DECLARE IN_FILE CURSOR FOR   
			SELECT 
				RIGHT(OUTPUT, 22)
			FROM 
				LEG_OUTPUT 
			WHERE 
				RIGHT(OUTPUT, 3) = 'csv'
			AND
				LEFT(RIGHT(OUTPUT, 22),18) LIKE 'Stat%'
			ORDER BY 
				ID;
  
	OPEN IN_FILE  
  
	FETCH NEXT FROM IN_FILE   
		INTO @IM_FILE_NAME   
  
	WHILE @@FETCH_STATUS = 0  
		BEGIN  
				IF @IM_FILE_NAME NOT IN (SELECT IM_FILE_NAME FROM IMPORTED)
					BEGIN
					INSERT INTO 
						IMPORT_LOG(LEG_KEY, INSERTED, UPDATED, U_I_STATUS)
							(SELECT 0, GETDATE(), GETDATE(), 'Schedule run');
					DECLARE @stm NVARCHAR(max)
					SET @stm = 'EXEC LEG_RAW_CIA N'''+@IM_FILE_NAME+''''; 
					EXEC sp_executesql @stm
					EXEC LEG_RAW_UP
					EXEC LEG_RAW_DIV
					EXEC LEG_RAW_DIV_IN
					EXEC LEG_INSERT;
					EXEC LEG_UPDATE
					INSERT INTO IMPORTED(IM_FILE_NAME) SELECT @IM_FILE_NAME
		END
	FETCH NEXT FROM IN_FILE     
			INTO @IM_FILE_NAME  
	END   
	CLOSE IN_FILE;  
	DEALLOCATE IN_FILE;

--SELECT * FROM IMPORTED;
GO

--CREATE PROCEDURE LEG_RAW_CIAU_DIVIDE

	CREATE PROCEDURE LEG_RAW_CIA @source varchar(30)
	AS 
	
	SET NOCOUNT ON; 
	DROP TABLE IF EXISTS RawAvia;
	
	 
	CREATE TABLE RawAvia
	(
		LEG_KEY varchar(10) NOT NULL,
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
		ACSTYP varchar(3) NOT NULL,
		ACTYP_NAME varchar(50) NOT NULL,
		CLASS varchar(1) NOT NULL,
		CONFIGA varchar(10) NOT NULL,
		SEATED varchar(10) NOT NULL,
		PAXCAT varchar(5) NOT NULL,
		ACTBD varchar(5) NOT NULL,
		ACTTFR varchar(5) NOT NULL,
		ACTTRT varchar(5) NOT NULL
	);
	

	
	DECLARE @sql NVARCHAR(max);
	SET @sql = N'
	BULK INSERT Stat.dbo.RawAvia
	FROM ''c:\Test_ETL\'+@source+'''
	WITH (
		FIRSTROW=2,
		FIELDQUOTE = ''"'',
		FIELDTERMINATOR = ''\t'',
		ROWTERMINATOR = ''0x0a'');'
	EXEC sp_executesql @sql


	UPDATE 
		RawAvia
			SET
				DATOP = 
					(SELECT TOP 1 CONVERT(date, R.DATOP, 101) FROM RawAvia AS R WHERE R.LEG_KEY = RawAvia.LEG_KEY)
	
	--RawAwia Altering
	ALTER TABLE 
			RawAvia 
				ADD 
					ATD varchar(20),
					TOFF varchar(20),
					TDWN varchar(20),
					ATA varchar(20),
					CNTR_DEF varchar(5)
GO

	CREATE PROCEDURE LEG_RAW_UP
	AS 
	
	SET NOCOUNT ON; 


	;WITH LEG_CTE (LEG_KEY, ATD, ATA, TOFF, TDWN, DIF_ATA_TDWN, CNTR_DEF)
			AS
			(
					SELECT 
						LEG_KEY,
						CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60) AS ATD,
						CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT) AS ATA,
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20 AS TOFF,
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT) AS TDWN,
							((CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT))
							-
							(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT))
							AS DIF_ATA_TDWN,
						(CASE 
							WHEN CTRYCDFR = 'NO' OR CTRYCDTO = 'NO' THEN 'NO'
							WHEN CTRYCDFR = 'SE' OR CTRYCDTO = 'SE' THEN 'SE'
							WHEN CTRYCDFR = 'FI' OR CTRYCDTO = 'FI' THEN 'FI'
							WHEN CTRYCDFR = 'IS' OR CTRYCDTO = 'IS' THEN 'IS'
							WHEN CTRYCDFR = 'DK' OR CTRYCDTO = 'DK' THEN 'DK'
							WHEN CTRYCDFR = 'FO' OR CTRYCDTO = 'FO' THEN 'FO'
							ELSE CTRYCDFR
						END) AS CNTR_DEF
					FROM
						RawAvia		
			)


		UPDATE RawAvia
			SET 
				RawAvia.ATD = DATEADD(mi,LEG_CTE.ATD,(CAST(DATOP AS datetime))),
				RawAvia.TOFF = 
					DATEADD(mi,
						CASE 
							WHEN LEG_CTE.DIF_ATA_TDWN>0
							THEN LEG_CTE.TOFF - LEG_CTE.DIF_ATA_TDWN
							ELSE LEG_CTE.TOFF
						END
					,(CAST(DATOP AS datetime))),
				RawAvia.TDWN = 
					DATEADD(mi,
						CASE 
							WHEN LEG_CTE.DIF_ATA_TDWN>0
							THEN LEG_CTE.ATA
							ELSE LEG_CTE.TDWN
						END
				,(CAST(DATOP AS datetime))),
				RawAvia.ATA = DATEADD(mi,LEG_CTE.ATA,(CAST(DATOP AS datetime))),
				RawAvia.CNTR_DEF = LEG_CTE.CNTR_DEF
				FROM
				LEG_CTE
		
			WHERE RawAvia.LEG_KEY = LEG_CTE.LEG_KEY

GO

	CREATE PROCEDURE LEG_RAW_DIV
	AS 
	
	SET NOCOUNT ON; 

	-- TEMPTABLE FOR INSERT

	DROP TABLE IF EXISTS RawAvia_INS;
	BEGIN
	CREATE TABLE RawAvia_INS
	(
		LEG_KEY varchar(10) NOT NULL,
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
		ACSTYP varchar(3) NOT NULL,
		ACTYP_NAME varchar(50) NOT NULL,
		CLASS varchar(1) NOT NULL,
		CONFIGA varchar(10) NOT NULL,
		SEATED varchar(10) NOT NULL,
		PAXCAT varchar(5) NOT NULL,
		ACTBD varchar(5) NOT NULL,
		ACTTFR varchar(5) NOT NULL,
		ACTTRT varchar(5) NOT NULL,
		ATD varchar(20),
		TOFF varchar(20),
		TDWN varchar(20),
		ATA varchar(20),
		CNTR_DEF varchar(5)
	)


	-- TEMPTABLE FOR UPDATE

	DROP TABLE IF EXISTS RawAvia_UP;

	CREATE TABLE RawAvia_UP
	(
		LEG_KEY varchar(10) NOT NULL,
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
		ACSTYP varchar(3) NOT NULL,
		ACTYP_NAME varchar(50) NOT NULL,
		CLASS varchar(1) NOT NULL,
		CONFIGA varchar(10) NOT NULL,
		SEATED varchar(10) NOT NULL,
		PAXCAT varchar(5) NOT NULL,
		ACTBD varchar(5) NOT NULL,
		ACTTFR varchar(5) NOT NULL,
		ACTTRT varchar(5) NOT NULL,
		ATD varchar(20),
		TOFF varchar(20),
		TDWN varchar(20),
		ATA varchar(20),
		CNTR_DEF varchar(5)
	)
	END
GO

	CREATE PROCEDURE LEG_RAW_DIV_IN
	AS 
	
	SET NOCOUNT ON; 
	INSERT INTO RawAvia_INS
		(
			LEG_KEY,
			FLTID,
			DATOP,
			LEGNO,
			DEPSTN,
			ARRSTN,
			CTRYCDFR,
			CTRYCDTO,
			BLHR,
			ABHR,
			GRPNO,
			STC,
			ACREG,
			ACVER,
			ACOWN,
			ACSTYP,
			ACTYP_NAME,
			CLASS,
			CONFIGA,
			SEATED,
			PAXCAT,
			ACTBD,
			ACTTFR,
			ACTTRT,
			ATD,
			TOFF,
			TDWN,
			ATA,
			CNTR_DEF 
		)
			SELECT 
				LEG_KEY,
				FLTID,
				DATOP,
				LEGNO,
				DEPSTN,
				ARRSTN,
				CTRYCDFR,
				CTRYCDTO,
				BLHR,
				ABHR,
				GRPNO,
				STC,
				ACREG,
				ACVER,
				ACOWN,
				ACSTYP,
				ACTYP_NAME,
				CLASS,
				CONFIGA,
				SEATED,
				PAXCAT,
				ACTBD,
				ACTTFR,
				ACTTRT,
				ATD,
				TOFF,
				TDWN,
				ATA,
				CNTR_DEF 
			FROM
				RawAvia AS R
			WHERE
				NOT EXISTS (SELECT LEG_KEY FROM LEG AS L WHERE L.LEG_KEY = R.LEG_KEY);


		

	INSERT INTO RawAvia_UP
		(
			LEG_KEY,
			FLTID,
			DATOP,
			LEGNO,
			DEPSTN,
			ARRSTN,
			CTRYCDFR,
			CTRYCDTO,
			BLHR,
			ABHR,
			GRPNO,
			STC,
			ACREG,
			ACVER,
			ACOWN,
			ACSTYP,
			ACTYP_NAME,
			CLASS,
			CONFIGA,
			SEATED,
			PAXCAT,
			ACTBD,
			ACTTFR,
			ACTTRT,
			ATD,
			TOFF,
			TDWN,
			ATA,
			CNTR_DEF 
		)
			SELECT 
				LEG_KEY,
				FLTID,
				DATOP,
				LEGNO,
				DEPSTN,
				ARRSTN,
				CTRYCDFR,
				CTRYCDTO,
				BLHR,
				ABHR,
				GRPNO,
				STC,
				ACREG,
				ACVER,
				ACOWN,
				ACSTYP,
				ACTYP_NAME,
				CLASS,
				CONFIGA,
				SEATED,
				PAXCAT,
				ACTBD,
				ACTTFR,
				ACTTRT,
				ATD,
				TOFF,
				TDWN,
				ATA,
				CNTR_DEF 
			FROM
				RawAvia AS R
			WHERE
				EXISTS (SELECT LEG_KEY FROM LEG AS L WHERE L.LEG_KEY = R.LEG_KEY);
	

GO
	--DROP PROCEDURE LEG_INSERT
	CREATE PROCEDURE LEG_INSERT
	AS 
	SET NOCOUNT ON; 
	BEGIN
	--STC table filling
	INSERT INTO 
		STC(STC_NAME, STC_FULL_NAME)
			SELECT 
				STC, ' '
			FROM 
				RawAvia_INS
			WHERE 
				NOT EXISTS 
					(SELECT 
						STC_NAME
					FROM 
						STC 
					WHERE 
						RawAvia_INS.STC=STC.STC_NAME 
					GROUP BY 
						STC_NAME) 
			GROUP BY 
				STC;	
	
	--COUNTRIES table filling

	INSERT INTO 
		COUNTRIES(COUNTRY_NAME, COUNTRY_FULL_NAME)
			SELECT 
				CTRYCDFR, ''
			FROM 
				RawAvia_INS 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY_NAME 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia_INS.CTRYCDFR=COUNTRIES.COUNTRY_NAME 
					GROUP BY 
						COUNTRY_NAME) 
			GROUP BY 
				CTRYCDFR
			UNION
			SELECT 
				CTRYCDTO, ' '
			FROM 
				RawAvia_INS 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY_NAME
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia_INS.CTRYCDTO=COUNTRIES.COUNTRY_NAME
					GROUP BY 
						COUNTRY_NAME) 
			GROUP BY 
				CTRYCDTO;
	
	--AIRCRAFT table filling

	INSERT INTO 
		AIRCRAFTS
			(ACREG, 
			ACVER, 
			ACOWN, 
			ACSTYP, 
			ACTYP_NAME)
					SELECT 
						ACREG, 
						ACVER, 
						ACOWN, 
						ACSTYP, 
						ACTYP_NAME 
					FROM 
						RawAvia_INS 
					WHERE 
						NOT EXISTS 
							(SELECT 
							ACREG, 
							ACVER, 
							ACOWN, 
							ACSTYP, 
							ACTYP_NAME 
							FROM 
							AIRCRAFTS 
							WHERE 
									RawAvia_INS.ACREG=AIRCRAFTS.ACREG 
								AND
									RawAvia_INS.ACVER=AIRCRAFTS.ACVER 
								AND
									RawAvia_INS.ACOWN=AIRCRAFTS.ACOWN 
								AND
									RawAvia_INS.ACSTYP=AIRCRAFTS.ACSTYP 
								AND
									RawAvia_INS.ACTYP_NAME=AIRCRAFTS.ACTYP_NAME
							GROUP BY 
								ACREG, 
								ACVER, 
								ACOWN, 
								ACSTYP, 
								ACTYP_NAME) 
					GROUP BY 
						ACREG, 
						ACVER, 
						ACOWN, 
						ACSTYP, 
						ACTYP_NAME;
	
	--STATIONS table filling

	INSERT INTO 
		STATIONS(STN_NAME, STN_FULL_NAME, STN_COUNTRY)
			SELECT 
				DEPSTN, ' ', (SELECT COUNTRY_KEY FROM COUNTRIES AS C WHERE C.COUNTRY_NAME = CTRYCDFR)
			FROM 
				RawAvia_INS 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STN_NAME 
					FROM 
						STATIONS 
					WHERE 
						RawAvia_INS.DEPSTN=STATIONS.STN_NAME 
					GROUP BY STN_NAME) 
			GROUP BY 
				DEPSTN, CTRYCDFR
			UNION
			SELECT 
				ARRSTN, ' ', (SELECT COUNTRY_KEY FROM COUNTRIES AS C WHERE C.COUNTRY_NAME = CTRYCDTO)
			FROM 
				RawAvia_INS 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STN_NAME
					FROM 
						STATIONS 
					WHERE 
						RawAvia_INS.ARRSTN=STATIONS.STN_NAME
					GROUP BY STN_NAME) 
			GROUP BY 
				ARRSTN, CTRYCDTO;

	--LEG table filling
	SET IDENTITY_INSERT LEG ON;
	INSERT INTO 
		LEG
			(
				AC_VER_TYP_KEY, 
				LEG_KEY, 
				FLTID, 
				DATOP, 
				LEGNO, 
				DEPSTN, 
				ARRSTN, 
				ATD, 
				ATA, 
				BLHR, 
				ABHR, 
				GRPNO, 
				STC, 
				TOFF, 
				TDWN, 
				CNTR_DEF, 
				_CREATED, 
				_CHANGED) 
						SELECT 
							(SELECT 
								TOP 1 AC_VER_TYP_KEY 
							FROM 
								AIRCRAFTS 
							WHERE 
									ACREG = RawAvia_INS.ACREG 
								AND 
									ACVER = RawAvia_INS.ACVER
								AND 
									ACOWN = RawAvia_INS.ACOWN
								AND 
									ACSTYP = RawAvia_INS.ACSTYP) 
							AS 
								AC_VER_TYP_KEY,
							LEG_KEY,
							FLTID, 
							DATOP, 
							LEGNO, 
							(SELECT 
								TOP 1 STN_KEY
							FROM 
								STATIONS 
							WHERE 
								STN_NAME = RawAvia_INS.DEPSTN) 
							AS 
								DEPSTN, 
							(SELECT 
								TOP 1 STN_KEY 
							FROM 
								STATIONS 
							WHERE 
								STN_NAME = RawAvia_INS.ARRSTN) 
							AS 
								ARRSTN, 
								ATD,
							--(SELECT TOP 1 ATD FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATD,
							--(SELECT TOP 1 ATA FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATA,
							--(SELECT TOP 1 ATA FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATA,
							ATA,
							BLHR,
							ABHR,
							GRPNO,
							(SELECT 
								TOP 1 STC_KEY 
							FROM 
								STC 
							WHERE 
								STC = RawAvia_INS.STC) 
							AS 
								STC,
							--(SELECT TOP 1 TOFF FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TOFF, 
							--(SELECT TOP 1 TOFF FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TOFF,
							TOFF,
							--(SELECT TOP 1 TDWN FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TDWN,
							--(SELECT TOP 1 TDWN FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TDWN,
							TDWN,
							--(SELECT TOP 1 CNTR_DEF FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS CNTR_DEF,
							(SELECT 
								TOP 1 COUNTRY_KEY 
							FROM 
								COUNTRIES 
							WHERE 
								COUNTRY_NAME = RawAvia_INS.CNTR_DEF) 
							AS 
								CNTR_DEF,
							GETDATE(),
							GETDATE()
						FROM 
							RawAvia_INS
						GROUP BY 
							LEG_KEY, 
							FLTID, 
							DATOP, 
							LEGNO, 
							DEPSTN, 
							ARRSTN,  
							BLHR, 
							ABHR, 
							GRPNO, 
							STC, 
							ACREG,
							ACVER,
							ACOWN,
							ACSTYP,
							ATD,
							ATA,
							TOFF,
							TDWN,
							CNTR_DEF
						ORDER BY 
							LEG_KEY, 
							FLTID, 
							DATOP, 
							LEGNO, 
							DEPSTN, 
							ARRSTN;
	
	--LEG_CLASS filling

	INSERT INTO 
		LEG_CLASS(
			LEG_KEY,
			CLASS, 
			CONFIGA, 
			SEATED,
			_CREATED,
			_CHANGED
			) 
					SELECT
						LEG_KEY,
						CLASS, 
						CONFIGA, 
						SEATED,
						GETDATE(),
						GETDATE()
					FROM 
						RawAvia_INS
					GROUP BY 
						LEG_KEY,
						CLASS, 
						CONFIGA, 
						SEATED						
					ORDER BY 
						LEG_KEY,
						CLASS;

	----LEG_PAXCAT filling
	INSERT INTO 
		LEG_PAXCAT(
			LEG_KEY,
			PAXCAT, 
			ACTBD, 
			ACTTFR, 
			ACTTRT,
			_CREATED,
			_CHANGED
			) 
						SELECT
						LEG_KEY,
						PAXCAT, 
						PARSE(ACTBD AS int), 
						PARSE(ACTTFR AS int), 
						PARSE(ACTTRT AS int),
						GETDATE(),
						GETDATE()
						FROM RawAvia_INS
						GROUP BY 
							LEG_KEY,
							PAXCAT, 
							ACTBD, 
							ACTTFR, 
							ACTTRT
						ORDER BY 
							LEG_KEY,
							PAXCAT;

	INSERT INTO 
		IMPORT_LOG(LEG_KEY, INSERTED, UPDATED, U_I_STATUS)
			(SELECT LEG_KEY, GETDATE(), ' ', 'I' FROM RawAvia_INS GROUP BY LEG_KEY);
	DROP TABLE RawAvia_INS;
	END
	GO


	CREATE PROCEDURE LEG_UPDATE
	AS 
	SET NOCOUNT ON;
	BEGIN


		MERGE LEG AS L 
		USING(SELECT LEG_KEY, FLTID, DATOP, LEGNO, BLHR, ABHR, GRPNO FROM RawAvia_UP GROUP BY LEG_KEY, FLTID, DATOP, LEGNO, BLHR, ABHR, GRPNO) AS RUP  
		ON RUP.LEG_KEY=L.LEG_KEY
		WHEN MATCHED THEN
		UPDATE SET 
			L.FLTID = RUP.FLTID,
			L.DATOP = RUP.DATOP,
			L.LEGNO = RUP.LEGNO,
			L.BLHR = RUP.BLHR,
			L.ABHR = RUP.ABHR,
			L.GRPNO = RUP.GRPNO,
			L._CHANGED = GETDATE();

		MERGE LEG_CLASS AS LC 
		USING(SELECT LEG_KEY, CLASS, CONFIGA, SEATED FROM RawAvia_UP GROUP BY LEG_KEY, CLASS, CONFIGA, SEATED) AS RUP  
		ON RUP.LEG_KEY=LC.LEG_KEY AND  RUP.CLASS=LC.CLASS
		WHEN MATCHED THEN
		UPDATE SET 
			LC.CONFIGA = RUP.CONFIGA,
			LC.SEATED = RUP.SEATED,
			LC._CHANGED = GETDATE();

		MERGE LEG_PAXCAT AS LP 
		USING(SELECT LEG_KEY, PAXCAT, ACTBD, ACTTFR, ACTTRT FROM RawAvia_UP GROUP BY LEG_KEY, PAXCAT, ACTBD, ACTTFR, ACTTRT) AS RUP  
		ON RUP.LEG_KEY=LP.LEG_KEY AND  RUP.PAXCAT=LP.PAXCAT
		WHEN MATCHED THEN
		UPDATE SET 
			LP.ACTBD = CAST(RUP.ACTBD AS INT),
			LP.ACTTFR = CAST(RUP.ACTTFR AS INT),
			LP.ACTTRT = PARSE(RUP.ACTTRT AS int),
			LP._CHANGED = GETDATE();

	UPDATE 
		IMPORT_LOG
			SET
				(LEG_KEY, 
				UPDATED, 
				U_I_STATUS)
					(SELECT LEG_KEY, GETDATE(), 'U' FROM RawAvia_INS GROUP BY LEG_KEY);

	DROP TABLE RawAvia_UP;			
	END
	GO


	CREATE PROCEDURE LEG_ARCHIVE
	AS 
	SET NOCOUNT ON;
	BEGIN
		--INSERT TO ARHIVE

	INSERT INTO 
			LEG_AR(
				AC_VER_TYP_KEY, 
				LEG_KEY, 
				FLTID, 
				DATOP, 
				LEGNO, 
				DEPSTN, 
				ARRSTN, 
				ATD, 
				ATA, 
				BLHR, 
				ABHR, 
				GRPNO, 
				STC, 
				TOFF, 
				TDWN, 
				CNTR_DEF, 
				_CREATED, 
				_CHANGED
				)
					SELECT 
						AC_VER_TYP_KEY, 
						LEG_KEY, 
						FLTID, 
						DATOP, 
						LEGNO, 
						DEPSTN, 
						ARRSTN, 
						ATD, 
						ATA, 
						BLHR, 
						ABHR, 
						GRPNO, 
						STC, 
						TOFF, 
						TDWN, 
						CNTR_DEF, 
						_CREATED, 
						_CHANGED
					FROM 
						LEG
					WHERE
						DATEDIFF ( year , DATOP, GETDATE())> 3;
		DELETE FROM LEG WHERE DATEDIFF ( year , DATOP, GETDATE())> 3;


	INSERT INTO 
			LEG_PAXCAT_AR(
				LEG_KEY,
				PAXCAT,
				ACTBD,
				ACTTFR,
				ACTTRT,
				_CREATED,
				_CHANGED
				)
					SELECT 
						LEG_KEY,
						PAXCAT,
						ACTBD,
						ACTTFR,
						ACTTRT,
						_CREATED,
						_CHANGED
					FROM 
						LEG_PAXCAT
					WHERE
						LEG_KEY IN (SELECT LEG_KEY FROM LEG WHERE DATEDIFF ( year , DATOP, GETDATE())> 3);
		DELETE FROM LEG_PAXCAT_AR WHERE LEG_KEY IN (SELECT LEG_KEY FROM LEG WHERE DATEDIFF ( year , DATOP, GETDATE())> 3);


		INSERT INTO 
			LEG_CLASS_AR(
				LEG_KEY,
				CLASS,
				CONFIGA,
				SEATED,
				_CREATED,
				_CHANGED
				)
					SELECT 
						LEG_KEY,
						CLASS,
						CONFIGA,
						SEATED,
						_CREATED,
						_CHANGED
					FROM 
						LEG_CLASS
					WHERE
						LEG_KEY IN (SELECT LEG_KEY FROM LEG WHERE DATEDIFF ( year , DATOP, GETDATE())> 3);
		DELETE FROM LEG_CLASS_AR WHERE LEG_KEY IN (SELECT LEG_KEY FROM LEG WHERE DATEDIFF ( year , DATOP, GETDATE())> 3);
	END
	
	GO





















/*

	SELECT LEG_KEY, _CHANGED FROM LEG GROUP BY LEG_KEY, _CHANGED ORDER BY 2 DESC;
	SELECT LEG_KEY, _CHANGED FROM LEG_CLASS GROUP BY LEG_KEY, _CHANGED ORDER BY 2 DESC;
	SELECT LEG_KEY, _CHANGED FROM LEG_PAXCAT GROUP BY LEG_KEY, _CHANGED ORDER BY 2 DESC;

--SELECT @sql
--EXEC sp_executesql @sql;
--DROP PROCEDURE LEG_OUT
--DROP PROCEDURE LEG_CURSOR
--DROP PROCEDURE LEG_RAW_CIA
--DROP PROCEDURE LEG_RAW_UP
--DROP PROCEDURE LEG_RAW_DIV
--DROP PROCEDURE LEG_RAW_DIV_IN
--DROP PROCEDURE LEG_INSERT
--DROP PROCEDURE LEG_UPDATE
EXEC LEG_OUT;
EXEC LEG_CURSOR;
	--IN CURSOR
		EXEC LEG_RAW_CIA N'Stat20200901000000.csv';
		EXEC LEG_RAW_UP;
		EXEC LEG_RAW_DIV;
		EXEC LEG_RAW_DIV_IN;
		EXEC LEG_INSERT;
		EXEC LEG_UPDATE
*/
			
/*
	SELECT * FROM RawAvia_INS
	UNION ALL
	SELECT * FROM RawAvia_UP GROUP BY LEG_KEY
	*/
--SELECT * FROM RawAvia;


/*
DECLARE @IM_FILE_NAME NVARCHAR(max);
			DECLARE @stm NVARCHAR(max);
			SET @IM_FILE_NAME = 'Stat20200901000000.csv'
			SET @stm = 'EXEC LEG_RAW_CIA N'''+@IM_FILE_NAME+''''; SELECT @stm 

/*
		UPDATE 
			LEG_CLASS	
		SET
			--LEG_CLASS.CONFIGA = RawAvia_UP.CONFIGA,
			LEG_CLASS.SEATED = RawAvia_UP.SEATED,
			LEG_CLASS._CHANGED = GETDATE()
		FROM
			LEG_CLASS AS LC
		INNER JOIN
			RawAvia_UP AS RUP
		ON
			LC.LEG_KEY= RUP.LEG_KEY
		AND
			LC.CLASS = RUP.CLASS

		*/





DECLARE @sql NVARCHAR(max);
	--DECLARE @source NVARCHAR(30);
	--SET @source = 'Stat20200903143859.csv';
	SET @sql = N'
	BULK INSERT Stat.dbo.RawAvia
	FROM ''c:\Test_ETL\'+@source+'''
	WITH (
		FIRSTROW=2,
		FIELDQUOTE = ''"'',
		FIELDTERMINATOR = ''\t'',
		ROWTERMINATOR = ''0x0a'');'

	--SELECT @sql
	EXEC sp_executesql @sql;
	END



		--LEG with temptable

	CREATE TABLE #LEG_TIME
		(
			LEG_KEY int, 
			ATD int, 
			ATA int,
			TOFF int, 
			TDWN int, 
			DIF_ATA_TDWN int, 
			CNTR_DEF varchar(5)
		);
		
	--DROP TABLE #LEG_TIME

	WITH TIME_CTE (LEG_KEY, ATD, ATA, TOFF, TDWN, DIF_ATA_TDWN, CNTR_DEF)
			AS
			(
					SELECT 
						LEG_KEY,
						DATOP,
						CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60) AS ATD,
						CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT) AS ATA,
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20 AS TOFF,
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT) AS TDWN,
							((CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT))
							-
							(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT))
							AS DIF_ATA_TDWN,
						(CASE 
							WHEN DEPSTN = 'NO' OR ARRSTN = 'NO' THEN 'NO'
							WHEN DEPSTN = 'SE' OR ARRSTN = 'SE' THEN 'SE'
							WHEN DEPSTN = 'FI' OR ARRSTN = 'FI' THEN 'FI'
							WHEN DEPSTN = 'IS' OR ARRSTN = 'IS' THEN 'IS'
							WHEN DEPSTN = 'DK' OR ARRSTN = 'DK' THEN 'DK'
							WHEN DEPSTN = 'FO' OR ARRSTN = 'FO' THEN 'FO'
							ELSE DEPSTN
						END) AS CNTR_DEF
					FROM
						RawAvia		
			)
	


	
	/*
	INSERT INTO
		#LEG_TIME
			(LEG_KEY, ATD, ATA, TOFF, TDWN, DIF_ATA_TDWN, CNTR_DEF)*/
			SELECT
				LEG_KEY,
				DATEADD(mi,ATD,(CAST(DATOP AS datetime)))
			FROM
				TIME_CTE

				GO


	--LEG 
	


	/*
	/*
	WITH TIME_CTE (LEG_KEY, ATD, ATA, TOFF, TDWN, DIF_ATA_TDWN, CNTR_DEF)
			AS
			(
					SELECT 
						LEG_KEY,
						CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60) AS ATD,
						CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT) AS ATA,
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20 AS TOFF,
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT) AS TDWN,
							((CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT))
							-
							(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT))
							AS DIF_ATA_TDWN,
						(CASE 
							WHEN DEPSTN = 'NO' OR ARRSTN = 'NO' THEN 'NO'
							WHEN DEPSTN = 'SE' OR ARRSTN = 'SE' THEN 'SE'
							WHEN DEPSTN = 'FI' OR ARRSTN = 'FI' THEN 'FI'
							WHEN DEPSTN = 'IS' OR ARRSTN = 'IS' THEN 'IS'
							WHEN DEPSTN = 'DK' OR ARRSTN = 'DK' THEN 'DK'
							WHEN DEPSTN = 'FO' OR ARRSTN = 'FO' THEN 'FO'
							ELSE DEPSTN
						END) AS CNTR_DEF
					FROM
						RawAvia		
			)
	
	*/

	CREATE TABLE #LEG_TIME
		(
			LEG_KEY int, 
			ATD int, 
			ATA int,
			TOFF int, 
			TDWN int, 
			DIF_ATA_TDWN int, 
			CNTR_DEF varchar(5)
		);
		
	--DROP TABLE #LEG_TIME

	INSERT INTO
		#LEG_TIME
			(LEG_KEY, ATD, ATA, TOFF, TDWN, DIF_ATA_TDWN, CNTR_DEF)
			SELECT 
					LEG_KEY,
					CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60) AS ATD,
					CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT) AS ATA,
					(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20 AS TOFF,
					(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT) AS TDWN,
						((CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60))+20+CAST(CAST(ABHR AS float)*60 AS INT))
						-
						(CAST(SUBSTRING(FLTID, 4, 4) AS INT)%(24*60)+CAST(CAST(BLHR AS float)*60 AS INT))
						AS DIF_ATA_TDWN,
					(CASE 
						WHEN DEPSTN = 'NO' OR ARRSTN = 'NO' THEN 'NO'
						WHEN DEPSTN = 'SE' OR ARRSTN = 'SE' THEN 'SE'
						WHEN DEPSTN = 'FI' OR ARRSTN = 'FI' THEN 'FI'
						WHEN DEPSTN = 'IS' OR ARRSTN = 'IS' THEN 'IS'
						WHEN DEPSTN = 'DK' OR ARRSTN = 'DK' THEN 'DK'
						WHEN DEPSTN = 'FO' OR ARRSTN = 'FO' THEN 'FO'
						ELSE DEPSTN
					END) AS CNTR_DEF
				FROM
					RawAvia	
				GROUP BY 
					LEG_KEY, FLTID, BLHR, ABHR, DEPSTN, ARRSTN


		SELECT * FROM #LEG_TIME AS L
		INNER JOIN COUNTRIES AS C ON C.COUNTRY_NAME = L.CNTR_DEF
		ORDER BY 1;

		CREATE TABLE #CNTR_DEF 
		(
			LEG_KEY int,
			CNTR_DEF varchar(5) 
		);
	
		--DROP TABLE #CNTR_DEF

		INSERT INTO #CNTR_DEF (
			LEG_KEY,
			CNTR_DEF)
				SELECT 
					LEG_KEY,
					CNTR_DEF,
					C.COUNTRY_KEY
					--(SELECT COUNTRY_KEY FROM COUNTRIES AS CTR WHERE CTR.COUNTRY_NAME = #LEG_TIME.CNTR_DEF) AS CNTR_DEF
				FROM 
					#LEG_TIME AS L
					INNER JOIN COUNTRIES AS C ON C.COUNTRY_NAME = L.CNTR_DEF


	/*
	INSERT INTO 
		LEG
			(
				AC_VER_TYP_KEY, 
				LEG_KEY, 
				FLTID, 
				DATOP, 
				LEGNO, 
				DEPSTN, 
				ARRSTN, 
				ATD, 
				ATA, 
				BLHR, 
				ABHR, 
				GRPNO, 
				STC, 
				TOFF, 
				TDWN, 
				CNTR_DEF, 
				_CREATED, 
				_CHANGED) */
						SELECT 
							(SELECT 
								TOP 1 AC_VER_TYP_KEY 
							FROM 
								AIRCRAFTS 
							WHERE 
									ACREG = RawAvia.ACREG 
								AND 
									ACVER = RawAvia.ACVER
								AND 
									ACOWN = RawAvia.ACOWN
								AND 
									ACSTYP = RawAvia.ACSTYP) 
							AS 
								AC_VER_TYP_KEY,
							LEG_KEY,
							FLTID, 
							DATOP, 
							LEGNO, 
							(SELECT 
								TOP 1 STN_KEY
							FROM 
								STATIONS 
							WHERE 
								STN_NAME = RawAvia.DEPSTN) 
							AS 
								DEPSTN, 
							(SELECT 
								TOP 1 STN_KEY 
							FROM 
								STATIONS 
							WHERE 
								STN_NAME = RawAvia.ARRSTN) 
							AS 
								ARRSTN, 
							--(SELECT TOP 1 ATD FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATD,
							(SELECT TOP 1 ATD FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATD,
							--(SELECT TOP 1 ATA FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATA,
							(SELECT TOP 1 ATA FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS ATA,
							BLHR,
							ABHR,
							GRPNO,
							(SELECT 
								TOP 1 STC_KEY 
							FROM 
								STC 
							WHERE 
								STC = RawAvia.STC) 
							AS 
								STC,
							--(SELECT TOP 1 TOFF FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TOFF, 
							(SELECT TOP 1 TOFF FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TOFF, 
							--(SELECT TOP 1 TDWN FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TDWN,
							(SELECT TOP 1 TDWN FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS TDWN,
							--(SELECT TOP 1 CNTR_DEF FROM TIME_CTE AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS CNTR_DEF,
							(SELECT TOP 1 CNTR_DEF FROM #LEG_TIME AS CTE WHERE CTE.LEG_KEY = LEG_KEY) AS CNTR_DEF,
							GETDATE(),
							GETDATE()
						FROM 
							RawAvia
						GROUP BY 
							LEG_KEY, 
							FLTID, 
							DATOP, 
							LEGNO, 
							DEPSTN, 
							ARRSTN,  
							BLHR, 
							ABHR, 
							GRPNO, 
							STC, 
							ACREG,
							ACVER,
							ACOWN,
							ACSTYP
						ORDER BY 
							LEG_KEY, 
							FLTID, 
							DATOP, 
							LEGNO, 
							DEPSTN, 
							ARRSTN;
	GO
	*/
	GO

	*/