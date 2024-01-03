from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col
import pickle

spark = SparkSession \
    .builder \
    .appName("Dataframe Creation") \
    .getOrCreate()

crimes_schema = StructType([
    StructField("Date Rptd", DateType()),
    StructField("DATE OCC", DateType()),
    StructField("Vict Age", IntegerType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
])

crimes_df = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df = crimes_df.withColumn("Date Rptd", col("Date Rptd").cast(DateType()))
crimes_df = crimes_df.withColumn("DATE OCC", col("DATE OCC").cast(DateType()))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

crimes_df.printSchema()

print(f"Total number of lines: {crimes_df.count()}")

# Print the data types of each column
print("Column data types:")
print(crimes_df.dtypes)

with open('./crimes_df.pkl', 'wb') as file:
    pickle.dump(crimes_df, file)

