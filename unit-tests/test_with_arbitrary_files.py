# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from my_package.code1 import * # instead of %run ./Code1
from my_package.code2 import * # instead of %run ./Code2

# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *

class TestFixtureArbitraryFiles(NutterFixture):
  def __init__(self):
    self.code2_table_name = "my_data"
    self.code1_view_name = "my_cool_data"
    self.covid19_table_name = "covid19"
    # spark = SparkSession.builder.appName('Covid19 Metrics').getOrCreate()
    self.covid19_num_entries = 2127
    self.code1_num_entries = 100
    NutterFixture.__init__(self)
    
  def run_code1_arbitrary_files(self):
    generate_data1(spark, n = self.code1_num_entries, name = self.code1_view_name)
    
  def assertion_code1_arbitrary_files(self):
    df = spark.read.table(self.code1_view_name)
    assert(df.count() == self.code1_num_entries)
    
  def run_code2_arbitrary_files(self):
    generate_data2(table_name = self.code2_table_name)
    
  def assertion_code2_arbitrary_files(self):
    some_tbl = spark.sql(f'SELECT COUNT(*) AS total FROM {self.code2_table_name}')
    first_row = some_tbl.first()
    assert (first_row[0] == 10)

  def run_covid19_metrics(self):
    # covid19_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/covid19.csv")
    cols = ["Deaths", "Country_Region"]
    rows = [("5", "Canada"), ("15", "USA"), ("20", "Italy")]
    df = spark.createDataFrame(rows, cols)
    read_covid19_data(df)
    
  def assertion_covid19_metrics(self):
    cols = ["Deaths", "Country_Region", "Updated"]
    rows = [
        ("5", "Canada", "2020-01-26"),
        ("15", "USA", "2020-01-26"),
        ("20", "Italy", "2020-01-26"),
        ("10", "Canada", "2020-02-26"),
        ("50", "USA", "2020-02-26"), 
        ("25", "Italy", "2020-02-26")
    ]
    df = spark.createDataFrame(rows, cols)
    result = read_covid19_data(df)
    assert (result["Canada"] == 15)

  def after_code2_arbitrary_files(self):
    spark.sql(f"drop table {self.code2_table_name}")
    
  # we're using Chispa library here to compare the content of the processed dataframe with expected results
  def assertion_upper_columns_arbitrary_files(self):
    cols = ["col1", "col2", "col3"]
    df = spark.createDataFrame([("abc", "cef", 1)], cols)
    upper_df = upper_columns(df, cols)
    expected_df = spark.createDataFrame([("ABC", "CEF", 1)], cols)
    assert_df_equality(upper_df, expected_df)

# COMMAND ----------

result = TestFixtureArbitraryFiles().execute_tests()
print(result.to_string())
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)

# COMMAND ----------


