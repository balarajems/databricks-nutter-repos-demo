# Databricks notebook source
from pyspark.sql import SparkSession


def read_covid19_data(table_name="covid19"):
    spark = SparkSession.builder.appName('Covid19 Metrics').getOrCreate()

    df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balaraje@microsoft.com/covid19.csv")
    df.show()
    df.head(3)
    df.printSchema()

def read_covid19_data(df):
    # covid19_df.printSchema()
    rows_data = df.collect()
    deaths_by_country = {}
    
    for row in rows_data:
        country = row.Country_Region
        if country not in deaths_by_country:
            deaths_by_country[country] = 0
        deaths_by_country[country] += int(row.Deaths)
        
    return deaths_by_country
