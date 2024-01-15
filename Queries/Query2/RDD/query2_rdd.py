from pyspark.sql import SparkSession
import csv

sc = SparkSession \
    .builder \
    .appName("RDD query 2 execution final") \
    .config("spark.executor.instances", "4") \
    .getOrCreate() \
    .sparkContext

def is_morning(str_time):
    time = int(str_time)
    return 500 <= time and time <= 1159
    
def is_noon(str_time):
    time = int(str_time)
    return 1200 <= time and time <= 1659
    
def is_evening(str_time):
    time = int(str_time)
    return 1700 <= time and time <= 2059
    
def is_night(str_time):
    time = int(str_time)
    return ((2100 <= time and time <= 2359) or ( time >= 0 and  time <= 459))

def split_csv(line):
    return next(csv.reader([line]))

file_paths = ["Crime_Data_from_2010_to_2019.csv", "Crime_Data_from_2020_to_Present.csv"]
crimes_street_day_periods = sc.textFile(",".join(file_paths)) \
    .map(split_csv) \
    .filter(lambda x: x[15] == "STREET") \
    .map(lambda x: ("morning", 1) if is_morning(x[3]) else (("noon", 1) if is_noon(x[3]) else (("evening", 1) if is_evening(x[3]) else ("night", 1)))) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \

print(crimes_street_day_periods.collect())
