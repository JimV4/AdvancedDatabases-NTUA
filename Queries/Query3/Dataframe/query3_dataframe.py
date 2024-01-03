from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, when, regexp_extract, regexp_replace
import pickle

spark = SparkSession \
    .builder \
    .appName("Dataframe query 3 execution") \
    .getOrCreate() \

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("`Date Rptd`", col("Date Rptd").cast(DateType()))
crimes_df = crimes_df.withColumn("DATE OCC", col("DATE OCC").cast(DateType()))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

excluded_null_crimes = crimes_df.filter(crimes_df["Vict Descent"].isNotNull())

victims_alias = excluded_null_crimes.withColumn("Vict Descent", when(crimes_df["`Vict Descent`"] == "A", "Other Asian")
    .when(crimes_df["`Vict Descent`"] == "B", "Black") 
    .when(crimes_df["`Vict Descent`"] == "C", "Chinese")
    .when(crimes_df["`Vict Descent`"] == "D", "Cambodian")
    .when(crimes_df["`Vict Descent`"] == "F", "Filipino")
    .when(crimes_df["`Vict Descent`"] == "G", "Guamanian")
    .when(crimes_df["`Vict Descent`"] == "H", "Hispanic/Latin/Mexican")
    .when(crimes_df["`Vict Descent`"] == "I", "American Indian/Alaskan Native")
    .when(crimes_df["`Vict Descent`"] == "J", "Japanese")
    .when(crimes_df["`Vict Descent`"] == "K", "Korean")
    .when(crimes_df["`Vict Descent`"] == "L", "Laotian")
    .when(crimes_df["`Vict Descent`"] == "O", "Other")
    .when(crimes_df["`Vict Descent`"] == "P", "Pacific Islander")
    .when(crimes_df["`Vict Descent`"] == "S", "Samoan")
    .when(crimes_df["`Vict Descent`"] == "U", "Hawaiian")
    .when(crimes_df["`Vict Descent`"] == "V", "Vietnamese")
    .when(crimes_df["`Vict Descent`"] == "W", "White")
    .when(crimes_df["`Vict Descent`"] == "X", "Unknown")
    .when(crimes_df["`Vict Descent`"] == "Z", "Asian Indian"))

income_df = spark.read.csv("LA_income_2015.csv", header=True, inferSchema=True)
rev_geocoding_df = spark.read.csv("revgecoding.csv", header=True, inferSchema=True)

income_df = income_df.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", '\\$', ''))
income_df = income_df.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", ',', '').cast("int"))

highest_3_zip_codes = income_df.orderBy(col("Estimated Median Income").desc()).limit(3)
lowest_3_zip_codes = income_df.orderBy(col("Estimated Median Income").asc()).limit(3)

both_zip_codes = highest_3_zip_codes.union(lowest_3_zip_codes)

joined1 = victims_alias.alias("crimes").join(
    rev_geocoding_df.alias("geocoding"),
    (col("crimes.LAT") == col("geocoding.LAT")) & (col("crimes.LON") == col("geocoding.LON")),
    "inner"
)
#.select("geocoding.ZIPcode", "crimes.LAT", "crimes.LON", "crimes.`Vict Descent`")

joined2 = joined1.join(both_zip_codes, both_zip_codes["Zip Code"] == joined1["ZIPcode"], "inner")

joined2.groupBy("Vict Descent").count().select("Vict Descent", "count").orderBy(col("count").desc()).show() 


