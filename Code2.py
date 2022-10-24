# Databricks notebook source
from pyspark.sql import SparkSession


def generate_data2(table_name="my_data"):
  df = SparkSession.getActiveSession().range(0,10)
  df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def read_covid19_data(table_name="covid19"):
    spark = SparkSession.builder.appName('Covid19 Metrics').getOrCreate()

    df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balaraje@microsoft.com/covid19.csv")
    df.show()
    df.head(3)
    df.printSchema()
