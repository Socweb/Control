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

def create_user_view(df: DataFrame) -> DataFrame:
    messages = df.where("event_type='message' AND datetime IS NOT NULL") \
    .select(F.col('event.message_from').alias('user_id'),
            F.col('city'), F.col('datetime')
           ).distinct()
    
    act_city = messages \
    .withColumn(
        'user_messages_rank', F.row_number().over(Window().partitionBy(['user_id']).orderBy(F.desc('datetime')))
    ).where('user_messages_rank == 1') \
    .select(F.col('user_id'), F.col('city').alias('act_city'))
    
    home_city = messages. \
    withColumn("date_group_rank",
               F.row_number().over(Window.partitionBy("user_id", "city").orderBy("datetime"))) \
    .selectExpr('*', 'date_sub(datetime, date_group_rank) as date_group') \
    .groupBy("user_id", "city", "date_group") \
    .agg(F.count("*").alias("cnt_days")) \
    .select("user_id", "city", "cnt_days") \
    .where("cnt_days > 1") \
    .withColumn('max_cnt', F.max('cnt_days').over(Window.partitionBy("user_id", "city"))) \
    .where(F.col('cnt_days') == F.col('max_cnt')) \
    .drop('max_cnt', 'cnt_days') \
    .withColumn("home_city", F.first("city").over(Window.partitionBy("user_id"))) \
    .drop("city") \

    travel_count = messages \
    .groupBy("user_id") \
    .agg(F.count("*").alias("travel_count"))
    
    travel_array = messages \
    .orderBy("datetime") \
    .groupBy("user_id") \
    .agg(F.collect_list("city").alias("travel_array")) \
    .drop("city", "datetime")
    
    local_time = messages \
    .withColumn('message_rank_by_user',
                F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc('datetime')))
               ).where('message_rank_by_user == 1') \
    .select(F.col('user_id'), F.col('datetime')).orderBy(F.col('user_id')) \
    .withColumn('local_time', F.from_utc_timestamp(F.col("datetime"), (F.lit("Australia/Sydney")))) \
        .drop("datetime")
    
    total_user_view = act_city \
    .join(home_city, ["user_id"], 'left') \
    .join(travel_count, ["user_id"]) \
    .join(travel_array, ["user_id"]) \
    .join(local_time, ["user_id"])
    
    return total_user_view

df = spark.read.parquet('/user/flaviusoct/data/tmp/activities_new')
user_view = create_user_view(df)
user_view.write.mode('overwrite').parquet('/user/flaviusoct/prod/data/user_view')