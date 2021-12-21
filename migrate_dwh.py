from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, size
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

logging.basicConfig(format='%(asctime)s  %(levelname)s : %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)
app_log = logging.getLogger(__name__)
app_log.setLevel(logging.INFO)

conf = SparkConf()
spark = SparkSession.builder.appName('Statement-Mapping')\
                    .master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')

df_review = spark.read.format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres").option("dbtable", "public.review")\
                .option("user", "postgres").option("password", "qwerty").load().sortWithinPartitions("id")

df_weather_temp = spark.read.format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres").option("dbtable", "public.weather_temperature")\
                .option("user", "postgres").option("password", "qwerty").load().sortWithinPartitions("id")\
                .withColumn("weather_temp_id", col("id")).withColumn("weather_date", col("date")).drop("date", "id")

df_weather_preci = spark.read.format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres").option("dbtable", "public.weather_precipitation")\
                .option("user", "postgres").option("password", "qwerty").load().sortWithinPartitions("id")\
                .withColumn("weather_precip_id", col("id")).drop("id")

#Actually we can make data mart of weather using this data
df_weather = df_weather_temp.join(df_weather_preci, [df_weather_temp.weather_date == df_weather_preci.date], how='left').select("weather_temp_id", "date", "weather_precip_id")

#if you want to make it just the restaurant, you can add
dm_review = df_review.repartition(100).withColumn("review_id", col("id"))\
                .join(df_weather, [df_review.date_string == df_weather.date], how='left')
                .drop("date_string", "id", "text")\
                .write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
                .option("dbtable", "public.dm_review").option("user", "postgres").option("password", "qwerty").save()

app_log.info("Done")

# Here's the DDL to create dm_review

# CREATE TABLE IF NOT EXISTS public.DM_REVIEW
# (
#     review_id     VARCHAR(1024),
#     business_id VARCHAR(1024),
#     user_id VARCHAR(1024),
# 	weather_temp_id INTEGER,
# 	weather_precip_id INTEGER,
#     date VARCHAR(1024),
#     cool INTEGER,
#     funny INTEGER,
#     stars DECIMAL(3,2),
#     useful INTEGER,
# 	FOREIGN KEY (review_id) REFERENCES public.review(id),
#     FOREIGN KEY (business_id) REFERENCES public.business(id),
#     FOREIGN KEY (user_id) REFERENCES public.user(id),
# 	FOREIGN KEY (weather_temp_id) REFERENCES public.weather_temperature(id),
# 	FOREIGN KEY (weather_precip_id) REFERENCES public.weather_precipitation(id)
# );
