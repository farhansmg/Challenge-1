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

def DateString(x) :
    dt = datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    str_dt = str(dt.year) + str('%02d' % dt.month) + str('%02d' % dt.day)
    return str_dt

setDtStr = F.udf(lambda x : DateString(x), StringType())
size_ = F.udf(lambda x: len(x), IntegerType())

df_business = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/yelp_dataset_business_csv/*.csv').sortWithinPartitions("business_id")

df_business.withColumn("id", col("business_id")).drop("business_id").write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.business").option("user", "postgres").option("password", "qwerty").save()

df_user = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/yelp_dataset_user_csv/*.csv').sortWithinPartitions("user_id")

df_user.withColumn("id", col("user_id")).drop("user_id")\
            .withColumn("yelping_since", to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))\
            .write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.user").option("user", "postgres").option("password", "qwerty").save()

df_checkin = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/yelp_dataset_checkin_csv/*.csv')

df_checkin.write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.checkin").option("user", "postgres").option("password", "qwerty").save()

df_hours = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/yelp_dataset_hours_csv/*.csv')

df_hours.write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.hours").option("user", "postgres").option("password", "qwerty").save()

df_tip = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/yelp_dataset_tip_csv/*.csv')

df_tip.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))\
            .withColumn("compliment_count", col("compliment_count").cast("integer"))\
            .where(col("business_id").isNotNull()).where(col("user_id").isNotNull())\
            .where(col("compliment_count").isNotNull()).where(col("date").isNotNull())\
            .filter(~col("business_id").contains(" ")).filter(~col("user_id").contains(" "))\
            .filter(~col("business_id").like("Corr%")).filter(~col("business_id").like("Donu%")).filter(~col("business_id").like("BOO"))\
            .where(df_tip.business_id != 'And').where(df_tip.business_id != 'Wells')\
            .write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.tip").option("user", "postgres").option("password", "qwerty").save()

df_review = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/yelp_dataset_review_csv/*.csv')

df_review.where(col("user_id").isNotNull()).where(col("date").isNotNull()).filter(~col("business_id").contains(" ")).filter(~col("user_id").contains(" ")).filter(~col("date").contains("."))\
            .where(size_('date') == 19).filter(~col("date").contains("a")).filter(~col("date").contains("i")).filter(~col("date").contains("u")).filter(~col("date").contains("e")).filter(~col("date").contains("o"))\
            .withColumn("date_string", setDtStr('date')).withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))\
            .withColumn("stars", col("stars").cast(DoubleType())).withColumn("cool", col("cool").cast(IntegerType()))\
            .withColumn("funny", col("funny").cast(IntegerType())).withColumn("useful", col("useful").cast(IntegerType()))\
            .withColumn("id", col("review_id")).drop("review_id")\
            .write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.review").option("user", "postgres").option("password", "qwerty").save()

df_degrees = spark.read.format("csv").options(header='true', inferSchema='true')\
                .load('s3a://yelp_dataset/weather_temperature/*.csv')

df_degrees.write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
            .option("dbtable", "public.weather_degrees").option("user", "postgres").option("password", "qwerty").save()

df_precipitation = spark.read.format("csv").options(header='true', inferSchema='true')\
                    .load('s3a://yelp_dataset/weather_precipitation/*.csv')

df_precipitation.withColumn("precipitation", col("precipitation").cast(DoubleType()))\
                    .write.mode("append").format("jdbc").option("url", "jdbc:postgresql://0.0.0.0:15432/postgres")\
                    .option("dbtable", "public.weather_precipitation").option("user", "postgres").option("password", "qwerty").save()

app_log.info("Done")
