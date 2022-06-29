																
																
															--LESSON 1



	USE master; 
	GO

	DROP DATABASE IF EXISTS Stat;
	GO



															--CREATE DB Stat



	CREATE DATABASE Stat 
	ON 
		( NAME = Stat_dat,
		FILENAME = 'd:\SIGMA\DB\Stat.mdf', 
		SIZE = 10, 
		MAXSIZE = 200, 
		FILEGROWTH = 5 ) 
	LOG ON 
		( NAME = Stat_log,
		FILENAME = 'd:\SIGMA\DB\Stat.ldf', 
		SIZE = 5MB, 
		MAXSIZE = 250MB, 
		FILEGROWTH = 5MB ) ; 
	ALTER DATABASE Stat 
		COLLATE Latin1_General_CS_AS ;  
	GO 


-- Stat DB is used

	USE Stat; 
	GO


--table for logging(ETL and another processes)

	CREATE TABLE LOGS
	(
		ID int IDENTITY(1,1),
		DATE_OF_LOGGING date NOT NULL,
		LOG_DATA varchar(100)
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'STAT database and LOGS table was created';
	GO
	--SELECT * FROM LOGS;
	--DROP TABLE LOGS;



															--Creation table for RAW data



	CREATE TABLE RawAvia
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
		SEATED varchar(10) NULL
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'RAW table created';
	GO



								--RAW's data insertion from the first source file to the RawAvia table




	BULK INSERT Stat.dbo.RawAvia
	FROM 'd:\SIGMA\DB\Source1Avia_26845.csv'
	WITH (
		FIRSTROW=2,
		FIELDQUOTE = '"',
		FIELDTERMINATOR = '\t',
		ROWTERMINATOR = '0x0a');
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows inserted to the RAW table';
	GO


	

														   --LESSON 2


                                                        --Tables creation



	CREATE TABLE STATIONS
	(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		STATION varchar(30) NOT NULL
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'STATIONS table was created';
	GO

	CREATE TABLE COUNTRIES
	(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		COUNTRY varchar(30) NOT NULL
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'COUNTRIES table was created';
	GO

	CREATE TABLE STC
	(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		STC varchar(30) NOT NULL
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'STC table was created';
	GO

	CREATE TABLE AIRCRAFT
		(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		ACREG varchar(10) NOT NULL,
		ACVER varchar(10) NOT NULL,
		ACOWN varchar(3) NOT NULL,
		ACTYP varchar(3) NOT NULL,
		ACTYP_NAME varchar(50) NOT NULL
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'AIRCRAFT table was created';
	GO


	CREATE TABLE LEG
	(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		FLTID varchar(10) NOT NULL,
		DATOP date NOT NULL,
		LEGNO int NULL,
		DEPSTN int NOT NULL REFERENCES STATIONS(ID),
		ARRSTN int NOT NULL REFERENCES STATIONS(ID),
		CTRYCDFR int NOT NULL REFERENCES COUNTRIES(ID) index IX_LEG_COUNTRYFROM nonclustered,
		CTRYCDTO int NOT NULL REFERENCES COUNTRIES(ID) index IX_LEG_COUNTRYTO nonclustered,
		BLHR float NOT NULL,
		ABHR float NOT NULL,
		GRPNO varchar(10) NOT NULL,
		STC_ID int NOT NULL REFERENCES STC(ID) index IX_LEG_STC nonclustered,
		AIRCRAFT_ID int NOT NULL REFERENCES AIRCRAFT(ID) index IX_LEG_AIRCRAFT nonclustered,
		INDEX IX_LEG_NoCl NONCLUSTERED (FLTID, DATOP, DEPSTN, ARRSTN)
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'LEG table was created';
	GO

	CREATE TABLE LEG_CLASS
	(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		CLASS varchar(1) NOT NULL,
		CONFIGA int NOT NULL,
		SEATED int NOT NULL,
		LEG_ID int NOT NULL REFERENCES LEG(ID) index IX_LEG_CLASS_LEGID nonclustered
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'LEG_CLASS table was created';
	GO




                                                -- DATA INSERTION




--STC table filling

	INSERT INTO 
		STC(STC)
			SELECT 
			STC 
			FROM 
			RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STC 
					FROM 
						STC 
					WHERE 
					RawAvia.STC=STC.STC 
					GROUP BY 
						STC) 
			GROUP BY 
				STC;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in STC';
	GO


--COUNTRIES table filling

	INSERT INTO 
		COUNTRIES(COUNTRY)
			SELECT 
				CTRYCDFR 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia.CTRYCDFR=COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY 
				CTRYCDFR
			UNION
			SELECT 
				CTRYCDTO 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia.CTRYCDTO=COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY 
				CTRYCDTO;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in COUNTRIES';
	GO


--STATIONS table filling

	INSERT INTO 
		STATIONS(STATION)
			SELECT 
				DEPSTN 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						STATIONS 
					WHERE 
						RawAvia.DEPSTN=STATIONS.STATION 
					GROUP BY STATION) 
			GROUP BY 
				DEPSTN
			UNION
			SELECT 
				ARRSTN 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						STATIONS 
					WHERE 
						RawAvia.ARRSTN=STATIONS.STATION 
					GROUP BY STATION) 
			GROUP BY 
				ARRSTN;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in STATIONS';
	GO


--AIRCRAFT table filling

	INSERT INTO 
		AIRCRAFT
			(ACREG, 
			ACVER, 
			ACOWN, 
			ACTYP, 
			ACTYP_NAME)
					SELECT 
						ACREG, 
						ACVER, 
						ACOWN, 
						ACTYP, 
						ACTYP_NAME 
					FROM 
						RawAvia 
					WHERE 
						NOT EXISTS 
							(SELECT 
							ACREG, 
							ACVER, 
							ACOWN, 
							ACTYP, 
							ACTYP_NAME 
							FROM 
							AIRCRAFT 
							WHERE 
									RawAvia.ACREG=AIRCRAFT.ACREG 
								AND
									RawAvia.ACVER=AIRCRAFT.ACVER 
								AND
									RawAvia.ACOWN=AIRCRAFT.ACOWN 
								AND
									RawAvia.ACTYP=AIRCRAFT.ACTYP 
								AND
									RawAvia.ACTYP_NAME=AIRCRAFT.ACTYP_NAME
							GROUP BY 
								ACREG, 
								ACVER, 
								ACOWN, 
								ACTYP, 
								ACTYP_NAME) 
					GROUP BY 
						ACREG, 
						ACVER, 
						ACOWN, 
						ACTYP, 
						ACTYP_NAME;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in AIRCRAFT';
	GO


--LEG table filling

	INSERT INTO 
		LEG
			(FLTID, 
			DATOP, 
			LEGNO, 
			DEPSTN, 
			ARRSTN, 
			CTRYCDFR, 
			CTRYCDTO, 
			BLHR, 
			ABHR, 
			GRPNO, 
			STC_ID, 
			AIRCRAFT_ID) 
						SELECT 
							FLTID, 
							DATOP, 
							LEGNO, 
							(SELECT 
								TOP 1 ID 
							FROM 
								STATIONS 
							WHERE 
								STATION = RawAvia.DEPSTN) 
							AS 
								DEPSTN, 
							(SELECT 
								TOP 1 ID 
							FROM 
								STATIONS 
							WHERE 
								STATION = RawAvia.ARRSTN) 
							AS 
								ARRSTN, 
							(SELECT 
								TOP 1 ID 
							FROM 
								COUNTRIES 
							WHERE 
								COUNTRY = RawAvia.CTRYCDFR) 
							AS 
								CTRYCDFR,
							(SELECT 
								TOP 1 ID 
							FROM 
								COUNTRIES 
							WHERE 
								COUNTRY = RawAvia.CTRYCDTO) 
							AS 
								CTRYCDTO,
							BLHR,
							ABHR,
							GRPNO,
							(SELECT 
								TOP 1 ID 
							FROM 
								STC 
							WHERE 
								STC = RawAvia.STC) 
							AS 
								STC,
							(SELECT 
								TOP 1 ID 
							FROM 
								AIRCRAFT 
							WHERE 
									ACREG = RawAvia.ACREG 
								AND 
									ACVER = RawAvia.ACVER) 
							AS 
								AIRCRAFT
						FROM 
							RawAvia
						GROUP BY 
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
							ACVER
						ORDER BY 
							DATOP, 
							FLTID, 
							DEPSTN, 
							ARRSTN;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG';
	GO


--LEG_CLASS table filling

	INSERT INTO 
		LEG_CLASS(
			CLASS, 
			CONFIGA, 
			SEATED, 
			LEG_ID) 
					SELECT
					CLASS, 
					CONFIGA, 
					PARSE(SEATED AS int), 
						(SELECT 
							TOP 1 ID 
						FROM 
							LEG 
						WHERE 
								FLTID = RawAvia.FLTID 
							AND 
								DATOP = CAST(RawAvia.DATOP AS date) 
							AND 
								(SELECT 
									TOP 1 STATION 
								FROM 
									STATIONS 
								WHERE 
									ID = DEPSTN) = RawAvia.DEPSTN 
							AND 
								(SELECT 
									TOP 1 STATION 
								FROM 
									STATIONS 
								WHERE 
									ID = ARRSTN) = RawAvia.ARRSTN) 
						AS 
							FLTDI 
					FROM 
						RawAvia 
					GROUP BY 
						CLASS, 
						CONFIGA, 
						SEATED, 
						FLTID, 
						DATOP, 
						DEPSTN, 
						ARRSTN
					ORDER BY 
						DATOP, 
						FLTID;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG_CLASS';
	GO


--RAW table cleaning 

	TRUNCATE TABLE dbo.RawAvia;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'TABLE RawAvia was TRUNCATED.'+' - ' + CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in RawAvia';
	GO


--RAW's data insertion from the second source file to the RawAvia table

	BULK INSERT Stat.dbo.RawAvia
	FROM 'd:\SIGMA\DB\Source2Avia_36844.csv'
	WITH (
		FIRSTROW=1,
		FIELDQUOTE = '"',
		FIELDTERMINATOR = '\t',
		ROWTERMINATOR = '0x0a');
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'EXTRA rows inserted to the RavAvia table';
	GO





                                                -- DATA INSERTION




--STC table filling

	INSERT INTO 
		STC(STC)
			SELECT 
				STC 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STC 
					FROM 
						STC 
					WHERE 
						RawAvia.STC=STC.STC 
					GROUP BY 
						STC) 
			GROUP BY 
			STC;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in STC';
	GO


--COUNTRIES table filling

	INSERT INTO 
		COUNTRIES(COUNTRY)
			SELECT 
				CTRYCDFR 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia.CTRYCDFR=COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY 
				CTRYCDFR
			UNION
			SELECT 
				CTRYCDTO 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia.CTRYCDTO=COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY 
			CTRYCDTO;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in COUNTRIES';
	GO


--STATIONS table filling

	INSERT INTO 
		STATIONS(STATION)
			SELECT 
				DEPSTN 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						STATIONS 
					WHERE 
						RawAvia.DEPSTN=STATIONS.STATION 
					GROUP BY 
						STATION) 
			GROUP BY DEPSTN
			UNION
			SELECT 
				ARRSTN 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						STATIONS 
					WHERE 
						RawAvia.ARRSTN=STATIONS.STATION 
					GROUP BY 
						STATION) 
			GROUP BY ARRSTN;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in STATIONS';
	GO


--AIRCRAFT table filling

	INSERT INTO 
		AIRCRAFT
			(ACREG, 
			ACVER, 
			ACOWN, 
			ACTYP, 
			ACTYP_NAME)
					SELECT 
						ACREG, 
						ACVER, 
						ACOWN, 
						ACTYP, 
						ACTYP_NAME 
					FROM 
						RawAvia 
					WHERE 
						NOT EXISTS 
							(SELECT 
								ACREG, 
								ACVER, 
								ACOWN, 
								ACTYP, 
								ACTYP_NAME 
							FROM 
								AIRCRAFT 
							WHERE 
									RawAvia.ACREG=AIRCRAFT.ACREG 
								AND
									RawAvia.ACVER=AIRCRAFT.ACVER 
								AND
									RawAvia.ACOWN=AIRCRAFT.ACOWN 
								AND
									RawAvia.ACTYP=AIRCRAFT.ACTYP 
								AND
									RawAvia.ACTYP_NAME=AIRCRAFT.ACTYP_NAME
							GROUP BY 
								ACREG, 
								ACVER, 
								ACOWN, 
								ACTYP, 
								ACTYP_NAME) 
					GROUP BY 
						ACREG, 
						ACVER, 
						ACOWN, 
						ACTYP, 
						ACTYP_NAME;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in AIRCRAFT';
	GO


--LEG table filling

	INSERT INTO 
		LEG
			(FLTID, 
			DATOP, 
			LEGNO, 
			DEPSTN, 
			ARRSTN, 
			CTRYCDFR, 
			CTRYCDTO, 
			BLHR, 
			ABHR, 
			GRPNO, 
			STC_ID, 
			AIRCRAFT_ID) 
					SELECT 
						FLTID, 
						DATOP, 
						LEGNO, 
						(SELECT 
							TOP 1 ID 
						FROM 
							STATIONS 
						WHERE 
							STATION = RawAvia.DEPSTN) 
						AS 
							DEPSTN, 
						(SELECT 
							TOP 1 ID 
						FROM 
							STATIONS 
						WHERE 
							STATION = RawAvia.ARRSTN) 
						AS 
							ARRSTN, 
						(SELECT 
							TOP 1 ID 
						FROM 
							COUNTRIES 
						WHERE 
							COUNTRY = RawAvia.CTRYCDFR) 
						AS 
							CTRYCDFR,
						(SELECT 
							TOP 1 ID 
						FROM 
							COUNTRIES 
						WHERE 
							COUNTRY = RawAvia.CTRYCDTO) 
						AS 
							CTRYCDTO,
						BLHR,
						ABHR,
						GRPNO,
						(SELECT 
							TOP 1 ID 
						FROM 
							STC 
						WHERE 
							STC = RawAvia.STC) 
						AS 
							STC,
						(SELECT 
							TOP 1 ID 
						FROM 
							AIRCRAFT 
						WHERE 
								ACREG = RawAvia.ACREG 
							AND 
								ACVER = RawAvia.ACVER) 
						AS 
							AIRCRAFT
					FROM 
						RawAvia
					GROUP BY 
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
						ACVER
					ORDER BY 
						DATOP, 
						FLTID, 
						DEPSTN, 
						ARRSTN;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG';
	GO


--LEG_CLASS table filling

	INSERT INTO 
		LEG_CLASS
			(CLASS, 
			CONFIGA, 
			SEATED, 
			LEG_ID) 
					SELECT
					CLASS, 
					CONFIGA, 
					PARSE(SEATED AS int), 
						(SELECT 
							TOP 1 ID 
						FROM 
							LEG 
						WHERE 
								FLTID = RawAvia.FLTID 
							AND 
								DATOP = CAST(RawAvia.DATOP AS date) 
							AND 
								(SELECT 
									TOP 1 STATION 
								FROM 
									STATIONS 
								WHERE 
									ID = DEPSTN) = RawAvia.DEPSTN 
							AND 
								(SELECT 
									TOP 1 STATION 
								FROM 
									STATIONS 
								WHERE 
									ID = ARRSTN) = RawAvia.ARRSTN) 
								AS 
									FLTDI 
					FROM 
						RawAvia 
					GROUP BY 
						CLASS, 
						CONFIGA, 
						SEATED, 
						FLTID, 
						DATOP, 
						DEPSTN, 
						ARRSTN
					ORDER BY 
						DATOP, 
						FLTID;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG_CLASS';
	GO





																--LESSON 3


--old RAW's table dropping

	DROP TABLE IF EXISTS RawAvia;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'RawAvia table has been dropped';
	GO

--new raw data's table creation

	CREATE TABLE RawAvia
		(NEW_KEY varchar(20) NOT NULL,
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
		PAXCAT varchar(10) NULL,
		BD varchar(10) NULL,
		TRF varchar(10) NULL,
		TRT varchar(10) NULL
		);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'New RAW table with new columns created';
	GO

--RAW's data insertion from the flat file to the RawAvia table

	BULK INSERT Stat.dbo.RawAvia
	FROM 
		'd:\SIGMA\DB\Source3Avia_129805.csv'
	WITH (
		FIRSTROW=2,
		FIELDQUOTE = '"',
		FIELDTERMINATOR = '\t',
		ROWTERMINATOR = '0x0a');
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'EXTRA rows inserted to the RavAvia table';
	GO


-- LEG_PAXCAT table creation

	CREATE TABLE LEG_PAXCAT
	(
		ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		PAXCAT varchar(5) NOT NULL,
		BD int NOT NULL,
		TRF int NOT NULL,
		TRT int NOT NULL,
		LEG_ID int NOT NULL REFERENCES LEG(ID) index IX_LEG_PAXCAT_LEGID nonclustered
	);
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'LEG_PAXCAT table was created';
	GO


--adding a new field to the LEG table

	ALTER TABLE 
		LEG 
			ADD 
				NEW_KEY int DEFAULT 0 NOT NULL;

	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'LEG table was altered with a new NEW_KEY column';
	GO
	UPDATE
		LEG 
			SET 
				NEW_KEY = 4000000 + ID;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'NEW_KEY column was updated with custom values in LEG table. - '+CAST(@@ROWCOUNT AS varchar(10))+' - values added';
	GO





																--DATA INSERTION 



--STC table filling

	INSERT INTO 
		STC(STC)
			SELECT 
				STC 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STC 
					FROM 
						STC 
					WHERE 
						RawAvia.STC=STC.STC 
					GROUP BY 
						STC) 
			GROUP BY STC;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in STC';
	GO
	


--COUNTRIES table filling

	INSERT INTO 
		COUNTRIES(COUNTRY)
			SELECT 
			CTRYCDFR 
			FROM 
			RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia.CTRYCDFR=COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY CTRYCDFR
			UNION
			SELECT 
				CTRYCDTO 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						COUNTRY 
					FROM 
						COUNTRIES 
					WHERE 
						RawAvia.CTRYCDTO=COUNTRIES.COUNTRY 
					GROUP BY 
						COUNTRY) 
			GROUP BY CTRYCDTO;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in COUNTRIES';
	GO


--STATIONS table filling

	INSERT INTO 
		STATIONS(STATION)
			SELECT 
				DEPSTN 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						STATIONS 
					WHERE 
						RawAvia.DEPSTN=STATIONS.STATION 
					GROUP BY 
						STATION) 
			GROUP BY DEPSTN
			UNION
			SELECT 
				ARRSTN 
			FROM 
				RawAvia 
			WHERE 
				NOT EXISTS 
					(SELECT 
						STATION 
					FROM 
						STATIONS 
					WHERE 
						RawAvia.ARRSTN=STATIONS.STATION 
					GROUP BY 
						STATION) 
			GROUP BY ARRSTN;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in STATIONS';
	GO


--AIRCRAFT table filling

	INSERT INTO AIRCRAFT(ACREG, ACVER, ACOWN, ACTYP, ACTYP_NAME)
		SELECT 
			ACREG, 
			ACVER, 
			ACOWN, 
			ACTYP, 
			ACTYP_NAME 
		FROM RawAvia 
		WHERE 
			NOT EXISTS 
				(SELECT ACREG, ACVER, ACOWN, ACTYP, ACTYP_NAME 
				FROM AIRCRAFT 
				WHERE 
						RawAvia.ACREG=AIRCRAFT.ACREG 
					AND
						RawAvia.ACVER=AIRCRAFT.ACVER 
					AND
						RawAvia.ACOWN=AIRCRAFT.ACOWN 
					AND
						RawAvia.ACTYP=AIRCRAFT.ACTYP 
					AND
						RawAvia.ACTYP_NAME=AIRCRAFT.ACTYP_NAME
				GROUP BY 
					ACREG, 
					ACVER, 
					ACOWN, 
					ACTYP, 
					ACTYP_NAME) 
		GROUP BY 
			ACREG, 
			ACVER, 
			ACOWN, 
			ACTYP, 
			ACTYP_NAME;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in AIRCRAFT';
	GO

--LEG table filling

	INSERT INTO 
		LEG(
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
		STC_ID, 
		AIRCRAFT_ID, 
		NEW_KEY) 
					SELECT 
						FLTID, 
						DATOP, 
						LEGNO, 
						(SELECT TOP 1 ID FROM STATIONS WHERE STATION = RawAvia.DEPSTN) AS DEPSTN, 
						(SELECT TOP 1 ID FROM STATIONS WHERE STATION = RawAvia.ARRSTN) AS ARRSTN, 
						(SELECT TOP 1 ID FROM COUNTRIES WHERE COUNTRY = RawAvia.CTRYCDFR) AS CTRYCDFR,
						(SELECT TOP 1 ID FROM COUNTRIES WHERE COUNTRY = RawAvia.CTRYCDTO) AS CTRYCDTO,
						BLHR,
						ABHR,
						GRPNO,
						(SELECT TOP 1 ID FROM STC WHERE STC = RawAvia.STC) AS STC,
						(SELECT TOP 1 ID FROM AIRCRAFT WHERE ACREG = RawAvia.ACREG AND ACVER = RawAvia.ACVER) AS AIRCRAFT,
						NEW_KEY
					FROM RawAvia
					GROUP BY 
						NEW_KEY, 
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
						ACVER
					ORDER BY 
						DATOP, 
						FLTID, 
						DEPSTN, 
						ARRSTN;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG';
	GO

--LEG_CLASS table filling

	INSERT INTO 
		LEG_CLASS(
			CLASS, 
			CONFIGA, 
			SEATED, 
			LEG_ID) 
						SELECT
						CLASS, 
						CONFIGA, 
						PARSE(SEATED AS int), 
							(SELECT TOP 1 ID 
							FROM LEG 
							WHERE 
									FLTID = RawAvia.FLTID 
								AND 
									DATOP = CAST(RawAvia.DATOP AS date) 
								AND 
									(SELECT TOP 1 STATION FROM STATIONS WHERE ID = DEPSTN) = RawAvia.DEPSTN 
								AND 
									(SELECT TOP 1 STATION FROM STATIONS WHERE ID = ARRSTN) = RawAvia.ARRSTN) AS FLTID 
						FROM RawAvia 
						GROUP BY 
							CLASS, 
							CONFIGA, 
							SEATED, 
							FLTID, 
							DATOP, 
							DEPSTN, 
							ARRSTN
						ORDER BY 
							DATOP, 
							FLTID;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG_CLASS';
	GO

--LEG_PAXCAT table filling

	INSERT INTO 
		LEG_PAXCAT(
			PAXCAT, 
			BD, 
			TRF, 
			TRT, 
			LEG_ID) 
						SELECT
						PAXCAT, 
						PARSE(BD AS int), 
						PARSE(TRF AS int), 
						PARSE(TRT AS int),
							(SELECT TOP 1 ID 
							FROM LEG 
							WHERE 
									FLTID = RawAvia.FLTID 
								AND 
									DATOP = CAST(RawAvia.DATOP AS date) 
								AND 
									(SELECT TOP 1 STATION FROM STATIONS WHERE ID = DEPSTN) = RawAvia.DEPSTN 
								AND 
									(SELECT TOP 1 STATION FROM STATIONS WHERE ID = ARRSTN) = RawAvia.ARRSTN) AS ID

						FROM RawAvia 
						GROUP BY 
							PAXCAT, 
							BD, 
							TRF, 
							TRT, 
							FLTID, 
							DATOP, 
							DEPSTN, 
							ARRSTN
						ORDER BY 
							DATOP, 
							FLTID, 
							PAXCAT;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG_PAXCAT';
	GO

-- View's creation. It shows the total amount M-class passengers and C+Y class, also there is a summary of adults and children on every flight

	CREATE VIEW 
		FLIGHT_Mq_CYq_CHDINFq_view
			AS 
				SELECT 
				NEW_KEY,
				FLTID, 
				DATOP, 
				LEGNO,
				ST.STATION AS DEPSTN, 
				(SELECT STATION FROM STATIONS WHERE STATIONS.ID = L.ARRSTN) AS ARRSTN, 
				C.COUNTRY AS CTRYCDFR, 
				(SELECT COUNTRY FROM COUNTRIES WHERE COUNTRIES.ID = L.CTRYCDTO) AS CTRYCDTO,
				BLHR,
				ABHR,
				GRPNO,
				S.STC,
				A.ACREG,
				A.ACVER,
				A.ACOWN,
				A.ACTYP,
				A.ACTYP_NAME,
				(SELECT SEATED FROM LEG_CLASS AS LC WHERE L.ID = LC.LEG_ID AND CLASS = 'M') AS M_CLASS_SEATED,
				(SELECT SUM(SEATED) FROM LEG_CLASS AS LC WHERE L.ID = LC.LEG_ID AND LC.CLASS IN ('Y','C') GROUP BY LC.LEG_ID) AS CY_CLASS_SEATED,
				(SELECT SUM(LP.BD+LP.TRF+LP.TRT) FROM LEG_PAXCAT AS LP WHERE L.ID = LP.LEG_ID AND LP.PAXCAT NOT IN ('CHD','INF') GROUP BY LP.LEG_ID) AS ADULTS,
				(SELECT SUM(LP.BD+LP.TRF+LP.TRT) FROM LEG_PAXCAT AS LP WHERE L.ID = LP.LEG_ID AND LP.PAXCAT IN ('CHD','INF') GROUP BY LP.LEG_ID) AS CHILDREN
			FROM LEG AS L
				LEFT JOIN STC AS S
					ON L.STC_ID=S.ID
				LEFT JOIN STATIONS AS ST
					ON L.DEPSTN=ST.ID
				LEFT JOIN COUNTRIES AS C
					ON L.CTRYCDFR=C.ID
				LEFT JOIN AIRCRAFT AS A
					ON L.AIRCRAFT_ID=A.ID;
	GO
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'FLIGHT_Mq_CYq_CHDINFq_view created.';
	GO

--SELECT * FROM FLIGHT_Mq_CYq_CHDINFq_view ORDER BY NEW_KEY;
--DROP VIEW IF EXISTS FLIGHT_Mq_CYq_CHDINFq_view;




--Creation ITVF. It returns a table according to setted class's value

	CREATE FUNCTION 
		ufn_FLIGHT_CLASS_SEATED	
			(@var varchar(1)) 
				RETURNS TABLE
				AS
				RETURN
				(
					SELECT 
						NEW_KEY,
						FLTID, 
						DATOP, 
						LEGNO,
						ST.STATION AS DEPSTN, 
							(SELECT 
								STATION 
							FROM 
								STATIONS 
							WHERE 
								STATIONS.ID = L.ARRSTN) 
							AS 
								ARRSTN, 
						C.COUNTRY AS CTRYCDFR, 
							(SELECT 
								COUNTRY 
							FROM 
								COUNTRIES 
							WHERE 
								COUNTRIES.ID = L.CTRYCDTO) 
							AS 
								CTRYCDTO,
						BLHR,
						ABHR,
						GRPNO,
						S.STC,
						A.ACREG,
						A.ACVER,
						A.ACOWN,
						A.ACTYP,
						A.ACTYP_NAME,
							(SELECT 
								SEATED 
							FROM 
								LEG_CLASS 
							AS 
								LC 
							WHERE 
									L.ID = LC.LEG_ID 
								AND 
									CLASS = @var) 
							AS 
								SEATED
					FROM 
						LEG AS L
							LEFT JOIN STC AS S
								ON L.STC_ID=S.ID
							LEFT JOIN STATIONS AS ST
								ON L.DEPSTN=ST.ID
							LEFT JOIN COUNTRIES AS C
								ON L.CTRYCDFR=C.ID
							LEFT JOIN AIRCRAFT AS A
								ON L.AIRCRAFT_ID=A.ID);
	GO
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'ufn_FLIGHT_CLASS_SEATED function created.';
	GO

	--SELECT * FROM ufn_FLIGHT_CLASS_SEATED('M');
	--DROP FUNCTION IF EXISTS ufn_FLIGHT_CLASS_SEATED;
	GO










													
													--LESSON 4






									--Transaction with LEG altering and time data adding 

	BEGIN TRANSACTION

	BEGIN TRY
		
		--ALTERING LEG table with new time columns
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
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'LEG table was ALTERED with 4 new columns';

		--UPDATING LEG's new columns with time data 

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

	/*							--CCHECKING time indicators
		SELECT * FROM
			(SELECT 
				FLTID,
				BLHR,
				ATD,
				ATA,
				FORMAT(DATEADD(mi,DATEDIFF(mi, ATD, ATA),'00:00:00'), 'HH:mm') AS a,
				FORMAT(DATEADD(mi,CAST(CAST(BLHR AS float)*60 AS INT),'00:00:00'), 'HH:mm') AS b,
				CAST(BLHR AS float)*60 AS d,
				FORMAT(DATEADD(mi,DATEDIFF(mi, TOFF, TDWN),'00:00:00'), 'HH:mm') AS aa,
				FORMAT(DATEADD(mi,CAST(CAST(ABHR AS float)*60 AS INT),'00:00:00'), 'HH:mm') AS bb,
				CAST(ABHR AS float)*60 AS da
			FROM 
				LEG) AS c
		WHERE
			c.a <> c.b
			OR
			c.aa <> c.bb;
	*/
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in LEG. 4 new columns have been filled';

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


											-- COUNTRIES table updating by adding scandinavian indication and new indexes creating


	BEGIN TRANSACTION

		BEGIN TRY
		
		--ADD new column to COUNTRIES for scandinavian indication

		IF (SELECT COL_LENGTH('COUNTRIES', 'SCANDINAVIAN')) IS NULL
			BEGIN
				ALTER TABLE 
						COUNTRIES
							ADD
								SCANDINAVIAN int
			END
		
		INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'SCANDINAVIAN column added to COUNTRIES'
		

		--ADDITIONAL INDEXES 

		IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_LEG_COUNTRYFROM' AND object_id = OBJECT_ID('LEG'))
		BEGIN
			CREATE NONCLUSTERED INDEX IX_LEG_COUNTRYFROM
			ON LEG (CTRYCDFR);
			INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'IX_LEG_COUNTRYFROM index created';
		END

		IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_LEG_COUNTRYTO' AND object_id = OBJECT_ID('LEG'))
		BEGIN
			CREATE NONCLUSTERED INDEX IX_LEG_COUNTRYTO
			ON LEG (CTRYCDTO);
			INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'IX_LEG_COUNTRYTO index created';
		END

		IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_AIRCRAFT_TYPE' AND object_id = OBJECT_ID('AIRCRAFT'))
		BEGIN
			CREATE NONCLUSTERED INDEX IX_AIRCRAFT_TYPE
			ON AIRCRAFT (ACTYP);
			INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'IX_AIRCRAFT_TYPE index created';
		END
		
		--COUNTRIES table updating by adding scandinavian indication
	

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
			INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows affected in COUNTRIES. Scandinavian indication added';

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

	--Table partitioning
	/*
	Я б зробив партиціонування таблиці LEG помісячно чи поквартально(за два неповних місяці в LEG в наз 30000 записів, 
	думаю це малувато для партиції, а за квартал(десь 100000 набереться) буде норм). 
	При цьому у нас підлеглі LEG_CLASS i LEG_PAXCAT більші в 3 рази і в 6 відповідно, а з іншої сторони, там колонок небагато.
	Використав би RANGE RIGHT, так як це зручніше для дат(задати кінцеву дату), тоді як RANGE LEFT більше підійде для числових діапазонів(задаємо початкову дату).
	також партиціонувались би підлеглі таблиці LEG_CLASS i LEG_PAXCAT відповідно FOREIGN i PRIMARY ключів
	(думаю в цьому допоміг би кластерний індекс і там булоб RANGE LEFT).
	Індекси розбивались би аналогічно і зберігались би поруч.
	*/

	GO






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





	--SELECTing from whole base
	
	SELECT 
		NEW_KEY,
		FLTID, 
		DATOP, 
		LEGNO,
		ST.STATION AS DEPSTN, 
		(SELECT 
			STATION 
		FROM 
			STATIONS 
		WHERE 
			STATIONS.ID = L.ARRSTN) 
		AS 
			ARRSTN, 
		C.COUNTRY AS CTRYCDFR, 
		(SELECT 
			COUNTRY 
		FROM 
			COUNTRIES 
		WHERE 
			COUNTRIES.ID = L.CTRYCDTO) 
		AS 
			CTRYCDTO,
		ATD,
		TOFF,
		BLHR,
		ABHR,
		TDWN,
		ATA,
		GRPNO,
		S.STC,
		A.ACREG,
		A.ACVER,
		A.ACOWN,
		A.ACTYP,
		A.ACTYP_NAME,
		LC.CLASS,
		LC.CONFIGA,
		LC.SEATED,
		LP.PAXCAT,
		LP.BD,
		LP.TRF,
		LP.TRT
	FROM LEG AS L
		LEFT JOIN LEG_CLASS AS LC
			ON L.ID=LC.LEG_ID
		LEFT JOIN STC AS S
			ON L.STC_ID=S.ID
		LEFT JOIN STATIONS AS ST
			ON L.DEPSTN=ST.ID
		LEFT JOIN COUNTRIES AS C
			ON L.CTRYCDFR=C.ID
		LEFT JOIN AIRCRAFT AS A
			ON L.AIRCRAFT_ID=A.ID
		LEFT JOIN LEG_PAXCAT AS LP
			ON L.ID=LP.LEG_ID
	ORDER BY 
		DATOP DESC, 
		FLTID DESC;
	INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'were selected from whole database';
	SELECT * FROM LOGS;
	GO


	select @@version;

	
