# Databricks notebook source
# MAGIC %python
# MAGIC # Configure MLflow Experiment
# MAGIC mlflow_experiment_id = 866112
# MAGIC 
# MAGIC # Including MLflow
# MAGIC import mlflow
# MAGIC import mlflow.spark
# MAGIC 
# MAGIC import os
# MAGIC #print("MLflow Version: %s" % mlflow.__version__)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC fraud_data = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/fraud.csv')
# MAGIC 
# MAGIC display(fraud_data)

# COMMAND ----------

# MAGIC %python
# MAGIC fraud_data.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC # Calculate the differences between originating and destination balances
# MAGIC fraud_data = fraud_data.withColumn("orgDiff", fraud_data.newbalanceOrig - fraud_data.oldbalanceOrg).withColumn("destDiff", fraud_data.newbalanceDest - fraud_data.oldbalanceDest)
# MAGIC 
# MAGIC # Create temporary view
# MAGIC fraud_data.createOrReplaceTempView("financials")

# COMMAND ----------

# MAGIC %python
# MAGIC display(fraud_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Organize by Type
# MAGIC select type, count(1) from financials group by type

# COMMAND ----------

# MAGIC %sql
# MAGIC select type, sum(amount) from financials group by type

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import functions as F
# MAGIC 
# MAGIC # Rules to Identify Known Fraud-based
# MAGIC fraud_data = fraud_data.withColumn("label", 
# MAGIC                    F.when(
# MAGIC                      (
# MAGIC                        (fraud_data.oldbalanceOrg <= 56900) & (fraud_data.type == "TRANSFER") & (fraud_data.newbalanceDest <= 105)) | 
# MAGIC                        (
# MAGIC                          (fraud_data.oldbalanceOrg > 56900) & (fraud_data.newbalanceOrig <= 12)) | 
# MAGIC                            (
# MAGIC                              (fraud_data.oldbalanceOrg > 56900) & (fraud_data.newbalanceOrig > 12) & (fraud_data.amount > 1160000)
# MAGIC                            ), 1
# MAGIC                    ).otherwise(0))
# MAGIC 
# MAGIC # Calculate proportions
# MAGIC fraud_cases = fraud_data.filter(fraud_data.label == 1).count()
# MAGIC total_cases = fraud_data.count()
# MAGIC fraud_pct = 1.*fraud_cases/total_cases
# MAGIC 
# MAGIC # Provide quick statistics
# MAGIC print("Based on these rules, we have flagged %s (%s) fraud cases out of a total of %s cases." % (fraud_cases, fraud_pct, total_cases))
# MAGIC 
# MAGIC # Create temporary view to review data
# MAGIC fraud_data.createOrReplaceTempView("financials_labeled")

# COMMAND ----------

# MAGIC %sql
# MAGIC select label, count(1) as `Transactions`, sum(amount) as `Total Amount` from financials_labeled group by label

# COMMAND ----------

# MAGIC %sql
# MAGIC -- where sum(destDiff) >= 10000000.00
# MAGIC select nameOrig, nameDest, label, TotalOrgDiff, TotalDestDiff
# MAGIC   from (
# MAGIC      select nameOrig, nameDest, label, sum(OrgDiff) as TotalOrgDiff, sum(destDiff) as TotalDestDiff 
# MAGIC        from financials_labeled 
# MAGIC       group by nameOrig, nameDest, label 
# MAGIC      ) a
# MAGIC  where TotalDestDiff >= 1000000
# MAGIC  limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select type, label, count(1) as `Transactions` from financials_labeled group by type, label

# COMMAND ----------

# MAGIC %python
# MAGIC # Initially split our dataset between training and test datasets
# MAGIC (train, test) = fraud_data.randomSplit([0.8, 0.2], seed=12345)
# MAGIC 
# MAGIC # Cache the training and test datasets
# MAGIC train.cache()
# MAGIC test.cache()
# MAGIC 
# MAGIC # Print out dataset counts
# MAGIC print("Total rows: %s, Training rows: %s, Test rows: %s" % (fraud_data.count(), train.count(), test.count()))

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml import Pipeline
# MAGIC from pyspark.ml.feature import StringIndexer
# MAGIC from pyspark.ml.feature import OneHotEncoderEstimator
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC from pyspark.ml.classification import DecisionTreeClassifier
# MAGIC 
# MAGIC # Encodes a string column of labels to a column of label indices
# MAGIC indexer = StringIndexer(inputCol = "type", outputCol = "typeIndexed")
# MAGIC 
# MAGIC # VectorAssembler is a transformer that combines a given list of columns into a single vector column
# MAGIC va = VectorAssembler(inputCols = ["typeIndexed", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "orgDiff", "destDiff"], outputCol = "features")
# MAGIC 
# MAGIC # Using the DecisionTree classifier model
# MAGIC dt = DecisionTreeClassifier(labelCol = "label", featuresCol = "features", seed = 54321, maxDepth = 5)
# MAGIC 
# MAGIC # Create our pipeline stages
# MAGIC pipeline = Pipeline(stages=[indexer, va, dt])

# COMMAND ----------

# MAGIC %python
# MAGIC # View the Decision Tree model (prior to CrossValidator)
# MAGIC dt_model = pipeline.fit(train)
# MAGIC display(dt_model.stages[-1])
