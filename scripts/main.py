import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

from datetime import timedelta, datetime
import sys
import subprocess
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
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

def select_dates(start_date: str, end_date: str) -> list:
    try:
        start_date = datetime.fromisoformat(start_date)
        end_date = datetime.fromisoformat(end_date)
    except:
        print('Неверный формат даты!') # ПИСАТЬ В ЛОГ!!!!!
    # Список дат в формате ['/user/master/data/events/date=2020-10-02', ...]
    paths_list = []
    # Количество дней между датами
    delta = end_date - start_date
    
    if delta.days < 0:
        print ('Указан слишком маленький диапазон!')  # ПИСАТЬ В ЛОГ!!!!!
    for i in range(delta.days + 1):
        paths_list.append('/user/master/data/geo/events/date=' + (start_date + timedelta(i)).__str__()[:10])
    return paths_list

#  Добавить проверку на количество записей в лог
try:
    activities = spark.read.parquet(*select_dates(start_date, end_date)).sample(0.15)
except:
    print('Нет данных за указанный диапазон! Берём выборку за весь период')
    
    # Делаем максимальную выборку. Срезы - для фильтрации мусора
    files_list = [x[:-1] for x in subprocess.Popen("hdfs dfs -ls /user/master/data/geo/events |  awk '{print $8}'",
                                                   shell=True,
                                                   stdout=subprocess.PIPE,
                                                   stderr=subprocess.STDOUT).stdout.readlines()[2:]]
    # Декодируем, ибо байтовая строка
    first_date = min(files_list)[-10:].decode('utf-8')
    last_date = max(files_list)[-10:].decode('utf-8')
    activities = spark.read.parquet(*select_dates(first_date, last_date)).sample(0.005)

try:
    cities = spark.read.csv('/user/flaviusoct/data/coord', sep=';', header=True).withColumnRenamed('lat', 'ltt')
except:
    print('Не найден указанный файл!')

# Удаляем строки с пропущенными значениями координат
activities_new = activities.where("lat IS NOT NULL AND lon IS NOT NULL")

# Делаем одну колонку со временем
# и добавляем колонку с уникальным id
activities_new = activities_new \
.withColumn('datetime', F.coalesce(F.col('event.datetime'), F.col('event.message_ts')).cast('timestamp')) \
.orderBy(F.asc('datetime')) \
.withColumn('activity_id', F.monotonically_increasing_id())

# Поскольку значения координат в файле содержатся в текстовом формате, то приводим их к числовому
# И делаем тоже с идентификаторами
cities_new = cities \
.withColumn("ltt", F.regexp_replace(F.col("ltt"), pattern=',', replacement='.').cast("double")) \
.withColumn("lng", F.regexp_replace(F.col("lng"), pattern=',', replacement='.').cast("double")) \
.withColumn('id', F.col("id").cast("integer"))

# Присоединяем города к датафрейму с событиями через crossjoin, что позволит каждому событию
# создать по 24 записи, соответствующии городам. Определяем расстояние до какого из них меньше
activities_new = activities_new.crossJoin(cities_new) \
        .withColumn('distance', calculate_dist) \
        .withColumn("distance_rank",
                    F.row_number().over(Window.partitionBy("activity_id").orderBy("distance"))) \
        .where("distance_rank == 1")

# Записываем временный датафрейм в хранилище
activities_new.write.mode('overwrite').parquet('/user/flaviusoct/data/tmp/activities_new')