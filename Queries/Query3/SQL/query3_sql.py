from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, when

spark = SparkSession \
    .builder \
    .appName("SQL query 3 execution") \
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

exclude_nulls_query = "SELECT * FROM crimes WHERE `Vict Descent` IS NOT NULL"

excluded_null_crimes = spark.sql(exclude_nulls_query)
excluded_null_crimes.createOrReplaceTempView("excluded_null_crimes")

victims_alias_query = "SELECT *, \
                        CASE \
                            WHEN `Vict Descent` = 'A' THEN 'Other Asian' \
                            WHEN `Vict Descent` = 'B' THEN 'Black' \
                            WHEN `Vict Descent` = 'C' THEN 'Chinese' \
                            WHEN `Vict Descent` = 'D' THEN 'Cambodian' \
                            WHEN `Vict Descent` = 'F' THEN 'Filipino' \
                            WHEN `Vict Descent` = 'G' THEN 'Guamanian' \
                            WHEN `Vict Descent` = 'H' THEN 'Hispanic/Latin/Mexican' \
                            WHEN `Vict Descent` = 'I' THEN 'American Indian/Alaskan Native' \
                            WHEN `Vict Descent` = 'J' THEN 'Japanese' \
                            WHEN `Vict Descent` = 'K' THEN 'Korean' \
                            WHEN `Vict Descent` = 'L' THEN 'Laotian' \
                            WHEN `Vict Descent` = 'O' THEN 'Other' \
                            WHEN `Vict Descent` = 'P' THEN 'Pacific Islander' \
                            WHEN `Vict Descent` = 'S' THEN 'Samoan' \
                            WHEN `Vict Descent` = 'U' THEN 'Hawaiian' \
                            WHEN `Vict Descent` = 'V' THEN 'Vietnamese' \
                            WHEN `Vict Descent` = 'W' THEN 'White' \
                            WHEN `Vict Descent` = 'X' THEN 'Unknown' \
                            WHEN `Vict Descent` = 'Z' THEN 'Asian Indian' \
                        END AS `Victim Descent` \
                       FROM excluded_null_crimes"

victims_alias = spark.sql(victims_alias_query)
victims_alias.show(10)
victims_alias.createOrReplaceTempView("victims_alias")

income_df = spark.read.csv("LA_income_2015.csv", header=True, inferSchema=True)
income_df.createOrReplaceTempView("income")

rev_geocoding_df = spark.read.csv("revgecoding.csv", header=True, inferSchema=True)
rev_geocoding_df.createOrReplaceTempView("rev_geocoding")

remove_dollar_comma_query = "SELECT *, \
                            CAST(REPLACE(REPLACE(`Estimated Median Income`, '$', ''), ',', '') AS INTEGER) AS `Estimated Median Income 2`\
                            FROM income" 

remove_dollar_comma = spark.sql(remove_dollar_comma_query)
remove_dollar_comma.createOrReplaceTempView("remove_dollar_comma")

both_zip_codes_query = "WITH highest_3_zip_codes AS ( \
                        SELECT * \
                        FROM remove_dollar_comma \
                        ORDER BY `Estimated Median Income 2` DESC \
                        LIMIT 3 \
                        ) \
                        , lowest_3_zip_codes AS ( \
                        SELECT * \
                        FROM remove_dollar_comma \
                        ORDER BY `Estimated Median Income 2` ASC \
                        LIMIT 3 \
                        ) \
                        SELECT * \
                        FROM highest_3_zip_codes \
                        UNION \
                        SELECT * \
                        FROM lowest_3_zip_codes;"

both_zip_codes = spark.sql(both_zip_codes_query)
both_zip_codes.createOrReplaceTempView("both_zip_codes")

join1_query = "SELECT * \
               FROM victims_alias INNER JOIN rev_geocoding \
               ON victims_alias.LAT = rev_geocoding.LAT AND \
               victims_alias.LON = rev_geocoding.LON"

join1 = spark.sql(join1_query)
join1.createOrReplaceTempView("join1")

join2_query = "SELECT * \
               FROM join1 INNER JOIN both_zip_codes \
               ON join1.ZIPcode = both_zip_codes.`Zip Code`"

join2 = spark.sql(join2_query)
join2.createOrReplaceTempView("join2")

result_query = "SELECT `Victim Descent`, COUNT(*) AS count FROM join2 GROUP BY `Victim Descent` ORDER BY count DESC"
result = spark.sql(result_query)
result.show()

