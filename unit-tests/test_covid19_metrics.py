# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from my_package.metrics import * # instead of %run ./Code2



# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *

class TestFixtureArbitraryFiles(NutterFixture):
  def __init__(self):
    self.covid19_table_name = "covid19"
    # spark = SparkSession.builder.appName('Covid19 Metrics').getOrCreate()
    # self.covid19_num_entries = 2127
    self.code1_num_entries = 100
    NutterFixture.__init__(self)

  def run_covid19_metrics(self):
    # cols = ["Deaths", "Country_Region"]
    # rows = [("5", "Canada"), ("15", "USA"), ("20", "Italy")]
    # df = spark.createDataFrame(rows, cols)
    # read_covid19_data(df)
    pass
    
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

# COMMAND ----------

result = TestFixtureArbitraryFiles().execute_tests()
print(result.to_string())
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)

# COMMAND ----------


