// Databricks notebook source
// MAGIC %sql
// MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
// MAGIC CREATE TEMPORARY TABLE insurance2
// MAGIC USING csv
// MAGIC OPTIONS (path "/FileStore/tables/insurance.csv", header "true", mode "FAILFAST")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) FROM insurance2

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(sex), sex FROM insurance2 GROUP BY sex

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(sex), sex FROM insurance2 WHERE smoker='yes' GROUP BY sex

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT region, SUM(chargeS) AS SumCharges FROM insurance2 GROUP BY region ORDER BY SumCharges DESC
