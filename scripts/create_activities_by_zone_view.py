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

def create_activity_by_zone_view(df: DataFrame, act_type: str, reg: bool = False) -> DataFrame:
    new_df = ''
    if reg:
        new_df = df.where(f'event_type == "message" and datetime IS NOT NULL') \
        .withColumn(
            'msg_rank', F.row_number().over(
                Window.partitionBy('event.message_from').orderBy(F.asc('datetime'))
            )
        ).where('msg_rank == 1')
    else:
        new_df = df.where(f'event_type == "{act_type}" and datetime IS NOT NULL')

    new_df = new_df.withColumn('month', F.month(F.col('datetime'))) \
    .withColumn('week', F.weekofyear(F.col('datetime'))) \
    .select(F.col('month'), F.col('week'), F.col('id').alias('zone_id')) \
    .withColumn(f'month_{act_type}', F.count('*').over(Window.partitionBy('month', 'zone_id'))) \
    .withColumn(f'week_{act_type}', F.count('*').over(Window.partitionBy('week', 'zone_id'))) \
    .groupBy('month', 'week', 'zone_id') \
    .agg(F.first(f"week_{act_type}").alias(f"week_{act_type}"),
         F.first(f"month_{act_type}").alias(f"month_{act_type}"))          
    
    return new_df

def create_activities_by_zone_view(df: DataFrame) -> DataFrame:
    messages = create_activity_by_zone_view(df, 'message')
    subscriptions = create_activity_by_zone_view(df, 'subscription')
    reactions = create_activity_by_zone_view(df, 'reaction')
    registrations = create_activity_by_zone_view(df, 'registration', True)
    
    return messages \
.join(subscriptions, ["week", "month", "zone_id"], 'full') \
.join(reactions, ["week", "month", "zone_id"], 'full') \
.join(registrations, ["week", "month", "zone_id"], 'full') \
.orderBy("week", "month", "zone_id")

df = spark.read.parquet('/user/flaviusoct/data/tmp/activities_new')
activities_by_zone_view = create_activities_by_zone_view(df)
activities_by_zone_view.write.mode('overwrite').parquet('/user/flaviusoct/prod/data/activities_by_zone_view')