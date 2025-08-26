use DataWarehouse;


IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
CREATE DATABASE DataWarehouse;
go


create schema bronze;
go
create schema silver;
go
create schema gold;
go
