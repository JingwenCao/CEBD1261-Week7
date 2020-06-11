// Databricks notebook source

// COMMAND ----------

// MAGIC %sql
// MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
// MAGIC CREATE TEMPORARY TABLE insurance2
// MAGIC USING csv
// MAGIC OPTIONS (path "/FileStore/tables/insurance.csv", header "true", mode "FAILFAST")

// COMMAND ----------

// MAGIC %sql
// MAGIC select sex, count(*) from insurance2 group by 1

// COMMAND ----------


