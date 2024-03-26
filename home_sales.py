from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

import requests

spark = SparkSession.builder.appName("Home Sales Analysis").getOrCreate()

url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
response = requests.get(url)

# Assuming you have write access to the current directory
with open("home_sales_revised.csv", "wb") as file:
    file.write(response.content)
home_sales_df = spark.read.option("header", "true").csv("home_sales_revised.csv")

home_sales_df.createOrReplaceTempView("home_sales")


spark.sql("""
SELECT date, ROUND(AVG(price), 2) as avg_price
FROM home_sales
WHERE bedrooms = 4
GROUP BY date
ORDER BY date
""").show()

spark.sql("""
SELECT date_built, ROUND(AVG(price), 2) as avg_price
FROM home_sales
WHERE bedrooms = 3 AND bathrooms = 3
GROUP BY date_built
ORDER BY date_built
""").show()

spark.sql("""
SELECT date_built, ROUND(AVG(price), 2) as avg_price
FROM home_sales
WHERE bedrooms = 3 AND bathrooms = 3 AND floors = 2 AND sqft_lot >= 2000
GROUP BY date_built
ORDER BY date_built
""").show()

spark.sql("""
SELECT view, ROUND(AVG(price), 2) as avg_price
FROM home_sales
WHERE price >= 350000
GROUP BY view
HAVING AVG(price) >= 350000
ORDER BY view
""").show()


spark.catalog.cacheTable("home_sales")

spark.catalog.isCached("home_sales")


home_sales_df.write.partitionBy("date_built").parquet("C:/home_saless")

parquet_df = spark.read.parquet("C:/home_saless")
parquet_df.createOrReplaceTempView("home_sales_parquet")


spark.catalog.uncacheTable("home_sales")

spark.catalog.isCached("home_sales")
