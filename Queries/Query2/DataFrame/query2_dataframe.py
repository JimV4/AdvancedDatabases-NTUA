from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession \
    .builder \
    .appName("Dataframe query 2 execution final") \
    .config("spark.executor.instances", "4") \
    .getOrCreate() \

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

street_crimes_df = crimes_df.filter(crimes_df["Premis Desc"] == "STREET")
street_crimes_with_day_period_df = street_crimes_df.withColumn("Day Period", when(street_crimes_df["TIME OCC"].between(500, 1159), "morning") \
    .when(street_crimes_df["TIME OCC"].between(1200, 1659), "noon") \
    .when(street_crimes_df["TIME OCC"].between(1700, 2059), "evening") \
    .when(street_crimes_df["TIME OCC"].between(2100, 2359), "night").when(street_crimes_df["TIME OCC"].between(0, 459), "night")) 

street_crimes_with_day_period_df.groupBy("Day Period").count().select("Day Period", "count").orderBy(col("count").desc()).show()



