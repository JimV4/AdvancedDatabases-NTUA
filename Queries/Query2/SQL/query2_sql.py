from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, when
import pickle

spark = SparkSession \
    .builder \
    .appName("Dataframe query 2 execution") \
    .config("spark.executor.instances", "4") \
    .getOrCreate() \

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("Date Rptd", col("Date Rptd").cast(DateType()))
crimes_df = crimes_df.withColumn("DATE OCC", col("DATE OCC").cast(DateType()))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

crimes_df.createOrReplaceTempView("crimes")

street_crimes_query = "SELECT * FROM crimes WHERE `Premis Desc` == 'STREET'"

street_crimes = spark.sql(street_crimes_query)
street_crimes.createOrReplaceTempView("street_crimes")

day_periods_query = "SELECT `Day Period`, COUNT(*) AS count \
                    FROM ( \
                        SELECT *, \
                            CASE \
                                WHEN `TIME OCC` BETWEEN 500 AND 1159 THEN 'morning' \
                                WHEN `TIME OCC` BETWEEN 1200 AND 1659 THEN 'noon' \
                                WHEN `TIME OCC` BETWEEN 1700 AND 2059 THEN 'evening' \
                                WHEN `TIME OCC` BETWEEN 2100 AND 2359 OR (`TIME OCC` BETWEEN 0 AND 459) THEN 'night'  \
                            END AS `Day Period` \
                        FROM street_crimes \
                    ) \
                    GROUP BY `Day Period` \
                    ORDER BY count DESC "

result = spark.sql(day_periods_query)
result.show()