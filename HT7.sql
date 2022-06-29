USE Stat; 
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

--NEW_KEY updating

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
	







                                                         --VIEW








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

                           
													  

													  
													  
													  
													  
													  
													  --FUNCTION










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
		BLHR,
		ABHR,
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