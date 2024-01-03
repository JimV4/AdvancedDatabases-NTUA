from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession.builder.appName("SQL query 1 execution").getOrCreate()

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

crimes_df.createOrReplaceTempView("crimes")

max_crimes_per_month_and_year_query = """SELECT year, month, crime_total, `#`
                                        FROM (
                                            SELECT 
                                                YEAR(`DATE OCC`) AS year,
                                                MONTH(`DATE OCC`) AS month,
                                                COUNT(*) AS crime_total,
                                                ROW_NUMBER() OVER (PARTITION BY YEAR(`DATE OCC`) ORDER BY COUNT(*) DESC) as `#`
                                            FROM crimes
                                            GROUP BY YEAR(`DATE OCC`), MONTH(`DATE OCC`)
                                        ) ranked
                                        WHERE `#` <= 3
                                        ORDER BY year ASC, crime_total DESC"""

result = spark.sql(max_crimes_per_month_and_year_query)
result.show(result.count(), truncate=False)

