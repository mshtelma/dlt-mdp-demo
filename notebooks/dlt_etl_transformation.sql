-- Databricks notebook source
CREATE
OR REFRESH LIVE TABLE landing_transformed (
  CONSTRAINT NN_ID EXPECT (NAME IS NOT NULL) ON VIOLATION FAIL
  UPDATE
) AS
SELECT
  name,
  address,
  email,
  operation,
  operation_date
FROM
  LIVE.landing_cleaned
