from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, DateType
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Dataframe Creation") \
    .getOrCreate()

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("Date Rptd", col("Date Rptd").cast(DateType()))
crimes_df = crimes_df.withColumn("DATE OCC", col("DATE OCC").cast(DateType()))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

crimes_df.printSchema()

print(f"Total number of lines: {crimes_df.count()}")

print("Column data types:")
print(crimes_df.dtypes)


