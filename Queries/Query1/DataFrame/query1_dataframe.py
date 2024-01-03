from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession.builder.appName("Dataframe query 1 execution").getOrCreate()


crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("Date Rptd", F.to_date(F.col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("DATE OCC", F.to_date(F.col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("Vict Age", F.col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", F.col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", F.col("LON").cast(DoubleType())) 

# Using DataFrame API for the query
window_spec = Window.partitionBy("year").orderBy(F.desc("crime_total"))

result_df = crimes_df.groupBy(F.year("DATE OCC").alias("year"), F.month("DATE OCC").alias("month")) \
    .agg(F.count("*").alias("crime_total")) \
    .withColumn("#", F.row_number().over(window_spec)) \
    .filter(F.col("#") <= 3) \
    .orderBy("year", F.desc("crime_total"))

result_df.show(result_df.count(), truncate=False)
