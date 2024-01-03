from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, when
import pickle

spark = SparkSession \
    .builder \
    .appName("Dataframe query 2 execution") \
    .getOrCreate() \

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("`Date Rptd`", col("Date Rptd").cast(DateType()))
crimes_df = crimes_df.withColumn("DATE OCC", col("DATE OCC").cast(DateType()))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 


# 479840 values
street_crimes_df = crimes_df.filter(crimes_df["Premis Desc"] == "STREET")
print(street_crimes_df.count())
street_crimes_with_day_period_df = street_crimes_df.withColumn("Day Period", when(street_crimes_df["TIME OCC"].between(500, 1159), "morning") \
    .when(street_crimes_df["TIME OCC"].between(1200, 1659), "noon") \
    .when(street_crimes_df["TIME OCC"].between(1700, 2059), "evening") \
    .when(street_crimes_df["TIME OCC"].between(2100, 2359), "night").when(street_crimes_df["TIME OCC"].between(0, 459), "night")) \
    .filter(col("Day Period") \
    .isNotNull() & col("Day Period").isin("morning", "noon", "evening", "night"))

#street_crimes_with_day_period_df.groupBy("Day Period").agg({"Day Period": "count"}).show()

street_crimes_with_day_period_df.groupBy("Day Period").count().select("Day Period", "count").orderBy(col("count").desc()).show()


