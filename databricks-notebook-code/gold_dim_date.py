# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)
print(type(incremental_flag))
if incremental_flag == '0':
  print('Full Load')

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSION MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Columns 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@saleshimanshidatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT(Date_ID) as Date_ID
# MAGIC from parquet.`abfss://silver@saleshimanshidatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

df_src=spark.sql('''
select DISTINCT(Date_ID) as Date_ID
from parquet.`abfss://silver@saleshimanshidatalake.dfs.core.windows.net/carsales` 
''')


# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model Sink - Initial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    
    df_sink=spark.sql('''
    select dim_date_key, Date_ID
    from cars_catalog.gold.dim_date
    ''')


else:
    df_sink=spark.sql('''
    select 1 as dim_date_key, Date_ID
    from parquet.`abfss://silver@saleshimanshidatalake.dfs.core.windows.net/carsales`
    where 1=0
    ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter= df_src.join(df_sink, df_src.Date_ID == df_sink.Date_ID,'left')\
                 .select(df_sink.dim_date_key, df_src.Date_ID)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_date_key').isNotNull())


# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_date_key').isNull()).select(df_src['Date_ID'])


# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fecth the max Surrogate Key from existing table

# COMMAND ----------

if incremental_flag == '0':
    max_value=1
else:
    max_value_df= spark.sql('''
    select max(cast(dim_date_key as int)) as max_value
    from cars_catalog.gold.dim_date
    ''')
    max_value=max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key Column and ADD the max surrogate key

# COMMAND ----------

df_filter_new= df_filter_new.withColumn('dim_date_key', max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_filter_new_final=df_filter_new.select('dim_date_key','Date_ID')
df_filter_new_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new_final.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Slowly Changing Dimension Type -1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental Run
if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
   delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@saleshimanshidatalake.dfs.core.windows.net/dim_date')

   delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_date_key = src.dim_date_key')\
              .whenMatchedUpdateAll()\
              .whenNotMatchedInsertAll()\
              .execute()
  
# Initial Run
else:
    df_final.write.format('delta')\
              .mode('overwrite')\
              .option('path','abfss://gold@saleshimanshidatalake.dfs.core.windows.net/dim_date')\
              .saveAsTable('cars_catalog.gold.dim_date')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date

# COMMAND ----------

