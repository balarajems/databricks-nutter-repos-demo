from pyspark.sql import SparkSession

def generate_data2(table_name="my_data"):
  df = SparkSession.getActiveSession().range(0,10)
  df.write.format("delta").mode("overwrite").saveAsTable(table_name)


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