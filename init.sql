CREATE DATABASE CitiesService
GO

CREATE TABLE Cities
(
  Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  Name NVARCHAR(50),
  Country NVARCHAR(50)
)
GO

INSERT INTO Cities
  (Name, Country)
VALUES
  (N'Sydney', N'Australia'),
  (N'Berlin', N'Germany');
GO
