-- Create catalogs for different environments (dev, test, prod)
-- Adjust 'dev' to 'test' or 'prod' as needed for deployment
CREATE CATALOG IF NOT EXISTS dev_bronze;
CREATE CATALOG IF NOT EXISTS dev_control;

-- Create schemas within the bronze catalog for each source system
-- These schemas will hold the external tables for raw data from each source
CREATE SCHEMA IF NOT EXISTS dev_bronze.crm;
CREATE SCHEMA IF NOT EXISTS dev_bronze.erp;
CREATE SCHEMA IF NOT EXISTS dev_bronze.marketing;

-- Create schemas within the control catalog for metadata and logging
CREATE SCHEMA IF NOT EXISTS dev_control.metadata;
CREATE SCHEMA IF NOT EXISTS dev_control.logs;

-- Grant necessary permissions (example: adjust for specific roles/users)
-- GRANT USE CATALOG ON CATALOG dev_bronze TO `data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA dev_bronze.crm TO `data-engineers`;
-- GRANT CREATE TABLE ON SCHEMA dev_bronze.crm TO `data-engineers`;
-- GRANT SELECT ON ANY TABLE IN SCHEMA dev_bronze.crm TO `data-analysts`;

-- Note: The Databricks notebooks will handle creation of the watermark and log tables
-- within dev_control.metadata and dev_control.logs respectively.
