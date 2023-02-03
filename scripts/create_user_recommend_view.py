import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import sys
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = SparkSession \
.builder \
.master("yarn")\
.config("spark.driver.memory", "4g") \
.config("spark.executor.memory", "4g") \
.config("spark.dynamicAllocation.executorIdleTimeout", "10000s") \
.appName("Projecte") \
.getOrCreate()      

start_date = sys.argv[1]
end_date = sys.argv[2]

calculate_dist = F.lit(2*6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col("lat")) - F.radians(F.col("ltt"))) / 2), 2) +
            F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("ltt"))) *
            F.pow(F.sin((F.radians(F.col("lon")) - F.radians(F.col("lng"))) / 2), 2)
        )
    )

def create_user_recommend_view(df: DataFrame, start_date: str, end_date: str) -> DataFrame:

    user_channel_pairs = df \
    .where(
        f'datetime < "{end_date}" and datetime IS NOT NULL and event.subscription_channel IS NOT NULL'
    ).select(
        F.col('event.user').alias('user_id'), F.col('event.subscription_channel').alias('channel'), 
        F.col('event_type'), F.col('id').alias('zone_id')
    ).distinct()
    
    friends_pairs = df \
    .select('event.message_from', 'event.message_to') \
    .withColumn('user_pair_hash', F.hash(F.col('message_from') + F.col('message_to'))) \
    .drop_duplicates(subset=['user_pair_hash'])
    
    user_user_pairs = user_channel_pairs \
    .join(user_channel_pairs.select('user_id', 'channel').withColumnRenamed('user_id', 'user_id2'), ['channel'], 'inner') \
    .where('user_id != user_id2') \
    .withColumn('user_pair_hash', F.hash(F.col('user_id') + F.col('user_id2'))) \
    .drop_duplicates(subset=['user_pair_hash']) \
    .join(friends_pairs, ['user_pair_hash'], 'leftanti') \
    .drop('user_pair_hash') \
    .select('user_id', 'user_id2', 'channel', 'zone_id')
    
    target_day_activities = df \
    .where(f'datetime >= "{start_date}" and datetime < "{end_date}"') \
    .withColumn('total_user_id', F.coalesce(F.col('event.message_from'), F.col('event.user'))) \
    .withColumn('time_rank', F.row_number().over(Window.partitionBy('total_user_id').orderBy(F.desc('datetime')))) \
    .where('time_rank == 1') \
    .select('total_user_id', 'lat', 'lon')
    
    user_recommend = user_user_pairs \
    .join(target_day_activities, [user_user_pairs.user_id == target_day_activities.total_user_id], 'left') \
    .withColumnRenamed('lat', 'ltt') \
    .withColumnRenamed('lon', 'lng') \
    .join(
        user_user_pairs.select('user_id2', 'channel').join(
            target_day_activities, [user_user_pairs.user_id2 == target_day_activities.total_user_id], 'left'
        ), ['user_id2'], 'inner'
    ).select('user_id', 'user_id2', 'lat', 'lon', 'ltt', 'lng', 'zone_id') \
    .withColumn('distance', calculate_dist) \
    .drop_duplicates() \
    .withColumn('processed_dttm', F.current_timestamp()) \
    .select(
        F.col('user_id').alias('user_left'), F.col('user_id2').alias('user_right'),
        F.col('zone_id'), F.col('processed_dttm'),
        F.from_utc_timestamp(F.col("processed_dttm"), (F.lit("Australia/Sydney"))).alias('local_time')
    )
    
    return user_recommend

df = spark.read.parquet('/user/flaviusoct/data/tmp/activities_new')
user_recommend_view = create_user_recommend_view(df, start_date, end_date)
user_recommend_view.write.mode('overwrite').parquet('/user/flaviusoct/prod/data/user_recommend_view')