USE master; 
GO 
CREATE DATABASE Stat 
ON 
( NAME = Stat_dat,
FILENAME = 'd:\SIGMA\DB\L5\Stat.mdf', 
SIZE = 10, 
MAXSIZE = 50, 
FILEGROWTH = 5 ) 
LOG ON 
( NAME = Stat_log,
FILENAME = 'd:\SIGMA\DB\L5\Stat.ldf', 
SIZE = 5MB, 
MAXSIZE = 25MB, 
FILEGROWTH = 5MB ) ; 
GO
ALTER DATABASE Stat 
    COLLATE Latin1_General_CS_AS ;  
GO 