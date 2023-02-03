import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import airflow
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

'''
conf_dict = {
            "master": "yarn",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g"
        }
'''
default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2023, 2, 3),
                                }

dag_spark = DAG(
                        dag_id = "create_views",
                        default_args=default_args,
                        schedule_interval= '1 0 * * *',
                        )

# объявляем задачу с помощью SparkSubmitOperator
init_func = SparkSubmitOperator(
                        task_id='init_func',
                        dag=dag_spark,
                        #ссылка на запускаемую джобу: application ='/home/user/partition_overwrite.py'.
                        application ='/lessons/scripts/main.py' ,
                        #  режимы запуска, но здесь значения из Airflow Connections
                        conn_id= 'yarn_spark',
                        #application_args = ["2022-01-01", datetime.now().strftime('%Y-%m-%d')],
                        application_args = ["2022-06-01", "2022-06-12"],
                        # те же настройки, что и у SparkSession или SparkContext
                        #conf= conf_dict
                        )

create_user_view = SparkSubmitOperator(
                        task_id='create_user_view',
                        dag=dag_spark,
                        #ссылка на запускаемую джобу: application ='/home/user/partition_overwrite.py'.
                        application ='/lessons/scripts/create_user_view.py' ,
                        #  режимы запуска, но здесь значения из Airflow Connections
                        conn_id= 'yarn_spark',
                        application_args = ["2022-01-01", datetime.now().strftime('%Y-%m-%d')],
                        # те же настройки, что и у SparkSession или SparkContext
                        #conf= conf_dict
                        )

create_activities_by_zone_view = SparkSubmitOperator(
                        task_id='create_activities_by_zone_view',
                        dag=dag_spark,
                        #ссылка на запускаемую джобу: application ='/home/user/partition_overwrite.py'.
                        application ='/lessons/scripts/create_activities_by_zone_view.py' ,
                        #  режимы запуска, но здесь значения из Airflow Connections
                        conn_id= 'yarn_spark',
                        application_args = ["2022-01-01", datetime.now().strftime('%Y-%m-%d')],
                        # те же настройки, что и у SparkSession или SparkContext
                        #conf= conf_dict
                        )

create_user_recommend_view = SparkSubmitOperator(
                        task_id='create_user_recommend_view',
                        dag=dag_spark,
                        #ссылка на запускаемую джобу: application ='/home/user/partition_overwrite.py'.
                        application ='/lessons/scripts/create_user_recommend_view.py' ,
                        #  режимы запуска, но здесь значения из Airflow Connections
                        conn_id= 'yarn_spark',
                        application_args = ["2022-01-01", datetime.now().strftime('%Y-%m-%d')],
                        # те же настройки, что и у SparkSession или SparkContext
                        #conf= conf_dict
                        )

init_func >> create_user_view >> create_activities_by_zone_view >> create_user_recommend_view