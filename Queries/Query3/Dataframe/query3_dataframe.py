from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, when, regexp_extract, regexp_replace, year, to_date

spark = SparkSession \
    .builder \
    .appName("Dataframe query 3 execution 4 executors Final") \
    .config("spark.executor.instances", "4") \
    .getOrCreate() \

crimes_df = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

crimes_df = crimes_df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

# Filter year 2015 on crimes and filter nulls
excluded_null_crimes = crimes_df.filter((year(crimes_df["Date Rptd"]) == 2015) & crimes_df["Vict Descent"].isNotNull())

# Alias names for user-friendly results
victims_alias_crimes_df = excluded_null_crimes.withColumn("Vict Descent", when(crimes_df["`Vict Descent`"] == "A", "Other Asian")
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

# Read income and filter out $ and , in the income column. Keep records that are located on LA
income_df = spark.read.csv("LA_income_2015.csv", header=True, inferSchema=True)
income_df = income_df.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", '\\$', ''))
income_df = income_df.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", ',', '').cast("int"))
income_df = income_df.filter(col("Community").contains("Los Angeles"))

# Read geocoding and if a columnn has two zip codes, always keep the first
rev_geocoding_df = spark.read.csv("revgecoding.csv", header=True, inferSchema=True)
rev_geocoding_df = rev_geocoding_df.withColumn("ZIPcode", regexp_extract("ZIPcode", r"(\d+)", 1).cast("int"))

# Filter out zip codes where none crime occured 
rev_geocoding_zips_df = rev_geocoding_df.select("ZIPcode").distinct()
income_with_crimes_df = income_df.join(rev_geocoding_zips_df, income_df["Zip Code"] == rev_geocoding_zips_df["ZIPcode"], "inner")

# Find highest 3 and lowest 3 zip codes based on income 
highest_3_zip_codes = income_with_crimes_df.orderBy(col("Estimated Median Income").desc()).limit(3)
lowest_3_zip_codes = income_with_crimes_df.orderBy(col("Estimated Median Income").asc()).limit(3)

# Get zip code on crimes by joining crimes with geocoding on LAT, LON
joined1 = victims_alias_crimes_df.alias("victims_alias_crimes").join(
    rev_geocoding_df.alias("geocoding"),
    (col("victims_alias_crimes.LAT") == col("geocoding.LAT")) & (col("victims_alias_crimes.LON") == col("geocoding.LON")),
    "inner"
)

# Join the above with highest 3 and lowest 3 zip codes to get the result
print("Highest 3")
joined2 = joined1.join(highest_3_zip_codes, highest_3_zip_codes["Zip Code"] == joined1["ZIPcode"], "inner")
joined2 = joined2.groupBy("Vict Descent").count().select("Vict Descent", "count").orderBy(col("count").desc())
joined2.show()

print()

print("Lowest 3")
joined3 = joined1.join(lowest_3_zip_codes, lowest_3_zip_codes["Zip Code"] == joined1["ZIPcode"], "inner")
joined3 = joined3.groupBy("Vict Descent").count().select("Vict Descent", "count").orderBy(col("count").desc())
joined3.show() 

