# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING FROM BRONZE LAYER

# COMMAND ----------

df = spark.read.format('parquet')\
    .option('inferSchema', True)\
    .load('abfss://bronze@saleshimanshidatalake.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA TRANSFORMATION

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('model_category',split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_Sold', col('Units_Sold').cast(StringType())).display()

# COMMAND ----------

df.withColumn('Units_Sold', col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df= df.withColumn('RevPerUnit', col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #ANALYSIS

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #To analyse units sold for each branch every year

# COMMAND ----------

df.groupBy('Year','BranchName')\
    .agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold',ascending=[True,False]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Visualize units sold for each branch every year

# COMMAND ----------

display(df.groupBy('Year','BranchName')\
    .agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold',ascending=[True,False]))

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING TO SILVER LAYER

# COMMAND ----------

df.write.format('parquet')\
  .mode('overwrite')\
    .option('path','abfss://silver@saleshimanshidatalake.dfs.core.windows.net/carsales')\
      .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # QUERY SILVER LAYER DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM parquet.`abfss://silver@saleshimanshidatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

