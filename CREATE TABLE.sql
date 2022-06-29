USE Stat; 
GO 
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
INSERT INTO LOGS(LOG_DATA) SELECT CAST(GETDATE() AS varchar(20)) + ' -  '+'RAW table created';

