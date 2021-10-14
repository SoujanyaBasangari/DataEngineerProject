import json
import numpy as np
import os
import pandas as pd
import prefect
import requests
import uuid
from datetime import datetime, timedelta
from prefect import task, Flow, Parameter
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
sc = SparkContext('local')
spark = SparkSession(sc)


@task(max_retries=10, retry_delay=timedelta(seconds=10))
def extract(url: str) -> dict:
    try :
        ypath = url+"yellow_tripdata_*.csv"
        gpath = url+"green_tripdata_*.csv"
      
        taxi_schema = StructType([StructField("VendorID", IntegerType(), False),
                                  StructField("pickup_datetime", TimestampType(), False),
                                  StructField("dropoff_datetime", TimestampType(), False),
                                  StructField("store_and_fwd_flag", StringType(), False),
                                  StructField("RatecodeID", IntegerType(), False),
                                  StructField("PULocationID", IntegerType(), False),
                                  StructField("DOLocationID", IntegerType(), False),
                                  StructField("passenger_count", IntegerType(), False),
                                  StructField("trip_distance", FloatType(), False),
                                  StructField("fare_amount", FloatType(), False),
                                  StructField("extra", FloatType(), False),
                                  StructField("mta_tax", FloatType(), False),
                                  StructField("tip_amount", FloatType(), False),
                                  StructField("tolls_amount", FloatType(), False),
                                  StructField("ehail_fee", FloatType(), False),
                                  StructField("improvement_surcharge", FloatType(), False),
                                  StructField("total_amount", FloatType(), False),
                                  StructField("payment_type", IntegerType(), False),
                                  StructField("trip_type", IntegerType(), False)])

        yellow_df = spark.read.option("header", True)\
        .schema(taxi_schema) \
        .csv(ypath)\
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")\
        .withColumn("taxi_type", lit("yellow")) \
        .withColumn("ehail_fee", lit(0.0)) 
   
    
        green_df = spark.read.option("header", True)\
        .schema(taxi_schema) \
        .csv(gpath) \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")\
        .withColumn("taxi_type", lit("green"))

    except:
        raise Exception('No data fetched!')
    
    return yellow_df,green_df


@task
def transform(yellow_df: pd.DataFrame,green_df: pd.DataFrame):
    #Add hour column
    yellow_df = yellow_df.withColumn("pickup_hour", F.from_unixtime(F.unix_timestamp(col("pickup_datetime"),"yyyy-MM-dd hh:mm:ss"),"yyyy-MM-dd hh:00:00"))
    green_df = green_df.withColumn("pickup_hour", F.from_unixtime(F.unix_timestamp(col("pickup_datetime"),"yyyy-MM-dd hh:mm:ss"),"yyyy-MM-dd hh:00:00"))

    taxi_df = yellow_df.union(green_df)
    taxi_schema = StructType(
      [StructField("VendorID", IntegerType(), False),
      StructField("pickup_datetime", TimestampType(), False),
      StructField("dropoff_datetime", TimestampType(), False),
      StructField("store_and_fwd_flag", StringType(), False),
      StructField("RatecodeID", IntegerType(), False),
      StructField("PULocationID", IntegerType(), False),
      StructField("DOLocationID", IntegerType(), False),
      StructField("passenger_count", IntegerType(), False),
      StructField("trip_distance", FloatType(), False),
      StructField("fare_amount", FloatType(), False),
      StructField("extra", FloatType(), False),
      StructField("mta_tax", FloatType(), False),
      StructField("tip_amount", FloatType(), False),
      StructField("tolls_amount", FloatType(), False),
      StructField("ehail_fee", FloatType(), False),
      StructField("improvement_surcharge", FloatType(), False),
      StructField("total_amount", FloatType(), False),
      StructField("payment_type", IntegerType(), False),
      StructField("trip_type", IntegerType(), False),
      StructField("taxi_type", IntegerType(), False)])
    
    taxi_df.write.option("schema",taxi_schema).mode('append').parquet("https://cloud.uni-koblenz.de/s/tTcoPwsBdoXnWcG/parquet/taxi_df.parquet")

    avro_schema = { "type": "record",
    "name":"avro_schema",
    "type":"record",
        "fields":[
            {"type":"int", "name":"VendorID"},
            {"type":"datetime", "name":"pickup_datetime"}
            {"type":"datetime", "name":"dropoff_datetime"}
            {"type":"string", "name":"store_and_fwd_flag"}
            {"type":"int", "name":"RatecodeID"}
            {"type":"int", "name":"PULocationID"}
            {"type":"int", "name":"DOLocationID"}
            {"type":"int", "name":"passenger_count"}
            {"type":"float", "name":"trip_distance"}
            {"type":"float", "name":"fare_amount"}
            {"type":"float", "name":"extra"}
            {"type":"float", "name":"mta_tax"}
            {"type":"float", "name":"tip_amount"}
            {"type":"float", "name":"tolls_amount"}
            {"type":"float", "name":"ehail_fee"}
            {"type":"float", "name":"improvement_surcharge"}
            {"type":"float", "name":"total_amount"}
            {"type":"float", "name":"payment_type"}
            {"type":"float", "name":"trip_type"}
            {"type":"float", "name":"taxi_type"}
        ]
     }
    
    taxi_df.write.option("forceSchema", avro_schema).save("https://cloud.uni-koblenz.de/s/tTcoPwsBdoXnWcG/parquet/taxi_df.avro")
   
    # taxi_df_parquet = spark.read.parquet("https://cloud.uni-koblenz.de/s/tTcoPwsBdoXnWcG/parquet/taxi_df.parquet")
    
    # taxi_df_avro = sqlContext.read.format("com.databricks.spark.avro").load("https://cloud.uni-koblenz.de/s/tTcoPwsBdoXnWcG/parquet/taxi_df.avro")
    
    return taxi_df


@task
def load(taxi_df: pd.DataFrame, path: str) -> None:
    
    # If output is needed in csv 
    taxi_df.write.csv(path)
    #set variable to be used to connect the database
    database = "TestDB"
    table = "dbo.tbl_spark_df"
     #write the dataframe into a sql table
    taxi_df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://localhost/SQLEXPRESS;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

    #for updating
    #taxi_df.write.mode(SaveMode.Append).jdbc(JDBCurl,mySqlTable,connectionProperties)

def prefect_flow():
    with Flow(name='simple_etl_pipeline') as flow:
        param_url = Parameter(name='p_url', required=True)

        users = extract(url=param_url)
        df_users = transform(users)
        load(data=df_users, path=f'C:/Users/Desktop/users_{int(datetime.now().timestamp())}.csv')

    return flow


if __name__ == '__main__':
    flow = prefect_flow()
    flow.run(parameters={
        'p_url': 'https://cloud.uni-koblenz.de/apps/files/DOES_NOT_EXIST'
    })
