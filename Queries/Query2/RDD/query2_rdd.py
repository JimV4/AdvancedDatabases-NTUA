from pyspark.sql import SparkSession
import csv

sc = SparkSession \
    .builder \
    .appName("RDD query 2 execution") \
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

# .map(lambda x: x.split(",")) \


file_paths = ["Crime_Data_from_2010_to_2019.csv", "Crime_Data_from_2020_to_Present.csv"]
crimes_street_day_periods = sc.textFile(",".join(file_paths)) \
    .map(split_csv) \
    .filter(lambda x: x[15] == "STREET") \
    .map(lambda x: ("morning", 1) if is_morning(x[3]) else (("noon", 1) if is_noon(x[3]) else (("evening", 1) if is_evening(x[3]) else ("night", 1)))) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1]) \

print(crimes_street_day_periods.collect())

    
    
""" morning_counter = 0
noon_counter = 0
evening_counter = 0 
night_counter = 0

def count_crimes(row):
    global morning_counter, noon_counter, evening_counter, night_counter
    time_val = row[3]
    if is_morning(time_val): 
    	morning_counter +=1
    elif is_noon(time_val):
    	noon_counter += 1
    elif is_evening(time_val):
    	evening_counter += 1
    elif is_night(time_val):
    	night_counter += 1

for row in crimes_with_street.collect():
    count_crimes(row)

print("Number of crimes commited in morning hours: " + str(morning_counter))
print("Number of crimes commited in noon hours: "    + str(noon_counter))
print("Number of crimes commited in evening hours: " + str(evening_counter))
print("Number of crimes commited in night hours: "   + str(night_counter)) """


#for row in crimes_with_street_and_filled_time.collect():
#    print(row)
