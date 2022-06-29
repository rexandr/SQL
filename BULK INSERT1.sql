USE Stat; 
GO 

/*
BULK INSERT Stat.dbo.RawAvia
FROM 'd:\SIGMA\DB\L7\Source3Avia.csv'
WITH (
	FIRSTROW=2,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = '\t',
    ROWTERMINATOR = '0x0a');
INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'rows inserted to the RAW table';

*/

--SELECT * from RawAvia;

/*
TRUNCATE TABLE dbo.RawAvia;
INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), 'TABLE RawAvia was TRUNCATED';
*/

/*

BULK INSERT Stat.dbo.RawAvia
FROM 'd:\SIGMA\DB\L6\Source2Avia.csv'
WITH (
	FIRSTROW=1,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = '\t',
    ROWTERMINATOR = '0x0a');
INSERT INTO LOGS(DATE_OF_LOGGING, LOG_DATA) SELECT GETDATE(), CAST(@@ROWCOUNT AS varchar(10))+' - '+'EXTRA rows inserted to the RAW table';
*/

--SELECT @@ROWCOUNT;