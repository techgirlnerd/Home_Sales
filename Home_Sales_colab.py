{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import round, avg\n",
    "\n",
    "spark = SparkSession.builder.appName(\"Home Sales Analysis\").getOrCreate()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Read the `home_sales_revised.csv` data into a DataFrame."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "home_sales_df = spark.read.csv('home_sales_revised.csv', header=True, inferSchema=True)\n",
    "home_sales_df.createOrReplaceTempView(\"home_sales\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Perform analysis using SparkSQL queries."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Example query\n",
    "spark.sql(\"\"\"\n",
    "SELECT year, ROUND(AVG(price), 2) AS average_price\n",
    "FROM home_sales\n",
    "GROUP BY year\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Cache the `home_sales` DataFrame for efficient querying."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "spark.catalog.cacheTable(\"home_sales\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Verify the table is cached."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "print(spark.catalog.isCached(\"home_sales\"))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Write the DataFrame to a partitioned parquet file."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "home_sales_df.write.mode(\"overwrite\").partitionBy(\"date_built\").parquet(\"/path/to/home_sales_partitioned\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Read the partitioned parquet file and create a temporary view."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "parquet_df = spark.read.parquet(\"home_sales_partitioned\")\n",
    "parquet_df.createOrReplaceTempView(\"home_sales_parquet\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Uncache the original `home_sales` table."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "spark.catalog.uncacheTable(\"home_sales\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Verify the table is no longer cached."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "print(spark.catalog.isCached(\"home_sales\"))"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "
"mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
