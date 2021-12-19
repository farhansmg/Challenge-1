from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Basic logging
logging.basicConfig(format='%(asctime)s  %(levelname)s : %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)
app_log = logging.getLogger(__name__)
app_log.setLevel(logging.INFO)

# Create spark job, yes I'm using spark in here
conf = SparkConf()
spark = SparkSession.builder.appName('Converting-Json')\
                    .master("local[*]").getOrCreate()
sc = spark.sparkContext
# Delete unwanted log from spark
sc.setLogLevel('WARN')

# Business dataset is a nested json. So, here I'm propose 2 ways
df_business = spark.read.json("yelp_dataset/yelp_academic_dataset_business.json")
# First, this is my choice and for challenge purpose, I'm extracting the struct type so we still get the key inside of the struct, load it intu different areas
# for the raw table schema i'll show it at the end of this code
# And yes, I'm saving it to staging area which is s3
df_business.drop("attributes", "hours").repartition(50).write.mode("overwrite")\
            .option('header','true').csv("yelp_dataset/yelp_dataset_business_csv/")
df_business.select(col("attributes.*"))\
            .repartition(50).write.mode("overwrite")\
            .option('header','true').csv("yelp_dataset/yelp_dataset_attributes_csv/")
df_business.select(col("hours.*"))\
            .repartition(50).write.mode("overwrite")\
            .option('header','true').csv("yelp_dataset/yelp_dataset_hours_csv/")
# Second, you can get the data as raw as possible by cast it to string, but you will sacrifice your key inside the struct type
# df_business.withColumn("attributes_string", col("attributes").cast("string"))\
#                 .withColumn("hours_string", col("hours").cast("string"))\
#                  .drop("attributes", "hours").repartition(50).write.mode("overwrite")\
#                  .option('header','true').csv("s3a://yelp_dataset/yelp_dataset_business_csv/")
df_checkin = spark.read.json("yelp_dataset/yelp_academic_dataset_checkin.json")
# why i'm repartitioning it? the answer is for question no.7
df_checkin.repartition(50).write.mode("overwrite").option('header','true').csv("s3a://yelp_dataset/yelp_dataset_checkin_csv/")
df_review = spark.read.json("yelp_dataset/yelp_academic_dataset_review.json")
df_review.repartition(50).write.mode("overwrite").option('header','true').csv("s3a://yelp_dataset/yelp_dataset_review_csv/")
df_tip = spark.read.json("yelp_dataset/yelp_academic_dataset_tip.json")
df_tip.repartition(50).write.mode("overwrite").option('header','true').csv("s3a://yelp_dataset/yelp_dataset_tip_csv/")
df_user = spark.read.json("yelp_dataset/yelp_academic_dataset_user.json")
df_user.repartition(50).write.mode("overwrite").option('header','true').csv("s3a://yelp_dataset/yelp_dataset_user_csv/")
app_log.info("Done-converting-to-csv")

# root
#  |-- address: string (nullable = true)
#  |-- attributes: struct (nullable = true)
#  |    |-- AcceptsInsurance: string (nullable = true)
#  |    |-- AgesAllowed: string (nullable = true)
#  |    |-- Alcohol: string (nullable = true)
#  |    |-- Ambience: string (nullable = true)
#  |    |-- BYOB: string (nullable = true)
#  |    |-- BYOBCorkage: string (nullable = true)
#  |    |-- BestNights: string (nullable = true)
#  |    |-- BikeParking: string (nullable = true)
#  |    |-- BusinessAcceptsBitcoin: string (nullable = true)
#  |    |-- BusinessAcceptsCreditCards: string (nullable = true)
#  |    |-- BusinessParking: string (nullable = true)
#  |    |-- ByAppointmentOnly: string (nullable = true)
#  |    |-- Caters: string (nullable = true)
#  |    |-- CoatCheck: string (nullable = true)
#  |    |-- Corkage: string (nullable = true)
#  |    |-- DietaryRestrictions: string (nullable = true)
#  |    |-- DogsAllowed: string (nullable = true)
#  |    |-- DriveThru: string (nullable = true)
#  |    |-- GoodForDancing: string (nullable = true)
#  |    |-- GoodForKids: string (nullable = true)
#  |    |-- GoodForMeal: string (nullable = true)
#  |    |-- HairSpecializesIn: string (nullable = true)
#  |    |-- HappyHour: string (nullable = true)
#  |    |-- HasTV: string (nullable = true)
#  |    |-- Music: string (nullable = true)
#  |    |-- NoiseLevel: string (nullable = true)
#  |    |-- Open24Hours: string (nullable = true)
#  |    |-- OutdoorSeating: string (nullable = true)
#  |    |-- RestaurantsAttire: string (nullable = true)
#  |    |-- RestaurantsCounterService: string (nullable = true)
#  |    |-- RestaurantsDelivery: string (nullable = true)
#  |    |-- RestaurantsGoodForGroups: string (nullable = true)
#  |    |-- RestaurantsPriceRange2: string (nullable = true)
#  |    |-- RestaurantsReservations: string (nullable = true)
#  |    |-- RestaurantsTableService: string (nullable = true)
#  |    |-- RestaurantsTakeOut: string (nullable = true)
#  |    |-- Smoking: string (nullable = true)
#  |    |-- WheelchairAccessible: string (nullable = true)
#  |    |-- WiFi: string (nullable = true)
#  |-- business_id: string (nullable = true)
#  |-- categories: string (nullable = true)
#  |-- city: string (nullable = true)
#  |-- hours: struct (nullable = true)
#  |    |-- Friday: string (nullable = true)
#  |    |-- Monday: string (nullable = true)
#  |    |-- Saturday: string (nullable = true)
#  |    |-- Sunday: string (nullable = true)
#  |    |-- Thursday: string (nullable = true)
#  |    |-- Tuesday: string (nullable = true)
#  |    |-- Wednesday: string (nullable = true)
#  |-- is_open: long (nullable = true)
#  |-- latitude: double (nullable = true)
#  |-- longitude: double (nullable = true)
#  |-- name: string (nullable = true)
#  |-- postal_code: string (nullable = true)
#  |-- review_count: long (nullable = true)
#  |-- stars: double (nullable = true)
#  |-- state: string (nullable = true)
#  |-- attributes_string: string (nullable = true)
#  |-- hours_string: string (nullable = true)
