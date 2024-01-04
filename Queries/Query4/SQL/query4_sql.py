from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, udf
from pyspark.sql.types import DoubleType, IntegerType
from math import radians, cos, sin, asin, sqrt

def get_distance(longit_a, latit_a, longit_b, latit_b):
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    
    area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
    
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    
    distance = central_angle * radius
    return abs(round(distance, 4))

spark = SparkSession.builder.appName("SQL query 4.1 execution").config("spark.executor.instances", "4").getOrCreate()
get_distance_udf = udf(lambda lat1, lon1, lat2, lon2: get_distance(lat1, lon1, lat2, lon2))
spark.udf.register("get_distance", get_distance_udf)

crimes_df1 = spark.read.csv("Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df2 = spark.read.csv("Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crimes_df = crimes_df1.union(crimes_df2)

crimes_df = crimes_df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) 
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast(DoubleType())) 
crimes_df = crimes_df.withColumn("LON", col("LON").cast(DoubleType())) 

police_departments_df = spark.read.csv("LAPD_Police_Stations.csv", header=True, inferSchema=True)
police_departments_df = police_departments_df.withColumn("X", col("X").cast(DoubleType())) 
police_departments_df = police_departments_df.withColumn("Y", col("Y").cast(DoubleType())) 

crimes_df.createOrReplaceTempView("crimes")
police_departments_df.createOrReplaceTempView("departments")

#################### AVERAGE DISTANCE FROM DEPARTMENT THAT UNDERTOOK THE INVESTIGATION FOR FIREARM CRIMES PER YEAR ####################

firearm_crimes_query = """SELECT * FROM crimes 
                                WHERE (lat <> 0 or lon <> 0) and `Weapon Used Cd` LIKE '1__'"""

firearm_crimes = spark.sql(firearm_crimes_query)
firearm_crimes.createOrReplaceTempView("firearm_crimes")

firearm_crimes_distance_query = """SELECT YEAR(`DATE OCC`) AS year, CAST(get_distance(c.`LAT`, c.`LON`, d.`Y`, d.`X`) as DOUBLE) AS distance
                                    FROM firearm_crimes c
                                    JOIN departments d ON c.`AREA ` = d.`PREC`"""

firearm_crimes_distance = spark.sql(firearm_crimes_distance_query)
firearm_crimes_distance.createOrReplaceTempView("firearm_crimes_distance")

firearm_crimes_distance_query = """SELECT year, AVG(`distance`) as average_distance, COUNT(*) as `#`
            FROM firearm_crimes_distance
            GROUP BY year
            ORDER BY year ASC"""

firearm_crimes_distance = spark.sql(firearm_crimes_distance_query)

#################### AVERAGE DISTANCE FROM DEPARTMENT THAT UNDERTOOK THE INVESTIGATION FOR WEAPON CRIMES PER DEPARTMENT ####################

weapon_crimes_query = """SELECT * FROM crimes 
                                WHERE (lat <> 0 or lon <> 0) and `Weapon Used Cd` IS NOT NULL"""

weapon_crimes = spark.sql(weapon_crimes_query)
weapon_crimes.createOrReplaceTempView("weapon_crimes")

weapon_crimes_distance_query = """SELECT `DIVISION` as division, get_distance(c.`LAT`, c.`LON`, d.`Y`, d.`X`) AS distance
                                    FROM weapon_crimes c
                                    JOIN departments d ON c.`AREA ` = d.`PREC`"""

weapon_crimes_distance = spark.sql(weapon_crimes_distance_query)
weapon_crimes_distance.createOrReplaceTempView("weapon_crimes_distance")

weapon_crimes_distance_query = """SELECT division, AVG(`distance`) as average_distance, COUNT(*) as `#`
            FROM weapon_crimes_distance
            GROUP BY division
            ORDER BY `#` DESC"""

weapon_crimes_distance = spark.sql(weapon_crimes_distance_query)

#################### AVERAGE DISTANCE FROM CLOSEST DEPARTMENT FOR FIREARM CRIMES PER YEAR ####################

firearm_crimes_min_distance_query = """WITH ranked_distances as (
                                    SELECT `DR_NO` as crime, 
                                        YEAR(`DATE OCC`) as year, 
                                        CAST(get_distance(c.`LAT`, c.`LON`, d.`Y`, d.`X`) as DOUBLE) AS distance,
                                        ROW_NUMBER() OVER (PARTITION BY `DR_NO` ORDER BY CAST(get_distance(c.`LAT`, c.`LON`, d.`Y`, d.`X`) as DOUBLE) ASC) as distance_rank
                                    FROM firearm_crimes c
                                    JOIN departments d)
                                SELECT crime, year, distance
                                FROM ranked_distances
                                WHERE distance_rank = 1"""

firearm_crimes_min_distance = spark.sql(firearm_crimes_min_distance_query)
firearm_crimes_min_distance.createOrReplaceTempView("firearm_crimes_min_distance")

firearm_crimes_avg_distance_query = """SELECT year, AVG(`distance`) as average_distance, COUNT(*) as `#`
            FROM firearm_crimes_min_distance
            GROUP BY year
            ORDER BY year ASC"""

firearm_crimes_avg_distance = spark.sql(firearm_crimes_avg_distance_query)

#################### AVERAGE DISTANCE FROM CLOSEST DEPARTMENT FOR WEAPON CRIMES PER DEPARTMENT ####################

weapon_crimes_min_distance_query = """WITH ranked_distances as (
                                    SELECT `DR_NO` as crime, 
                                        `DIVISION` as division,
                                        CAST(get_distance(c.`LAT`, c.`LON`, d.`Y`, d.`X`) as DOUBLE) AS distance,
                                        ROW_NUMBER() OVER (PARTITION BY `DR_NO` ORDER BY CAST(get_distance(c.`LAT`, c.`LON`, d.`Y`, d.`X`) as DOUBLE) ASC) as distance_rank
                                    FROM weapon_crimes c
                                    JOIN departments d)
                                SELECT crime, division, distance
                                FROM ranked_distances
                                WHERE distance_rank = 1"""

weapon_crimes_min_distance = spark.sql(weapon_crimes_min_distance_query)
weapon_crimes_min_distance.createOrReplaceTempView("weapon_crimes_min_distance")

weapon_crimes_avg_distance_query = """SELECT division, avg(distance) as average_distance, COUNT(*) as `#`
                                FROM weapon_crimes_min_distance
                                GROUP BY division
                                ORDER BY `#` DESC"""

weapon_crimes_avg_distance = spark.sql(weapon_crimes_avg_distance_query)

#################### SHOW RESULTS ####################

print("Distance calculated from the police department that undertook the investigation:\n")
firearm_crimes_distance.show(firearm_crimes_distance.count(), truncate=False)
weapon_crimes_distance.show(weapon_crimes_distance.count(), truncate=False)

print("Distance calculated from the closest police department:\n")
firearm_crimes_avg_distance.show(firearm_crimes_avg_distance.count(), truncate=False)
weapon_crimes_avg_distance.show(weapon_crimes_avg_distance.count(), truncate=False)
