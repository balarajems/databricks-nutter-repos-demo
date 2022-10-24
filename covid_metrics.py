from pyspark.sql import SparkSession
import pyspark


spark = SparkSession.builder.appName('Covid19 Metrics').getOrCreate()

def read_covid19_data(table_name="covid19"):
    df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balaraje@microsoft.com/covid19.csv")
    df.show()
    df.head(3)
    df.printSchema()
