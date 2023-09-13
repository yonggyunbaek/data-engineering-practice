# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, date_format, to_date, regexp_replace, rank
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os
import shutil
import zipfile
import csv


# sys.stdout.reconfigure(encoding='utf-8')

def initialize_spark():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    return spark

def unzip_data():
    # zip 파일의 경로 (Path to the zip files)
    folder_path = "data"
    filenames = os.listdir(folder_path)

    # zip 파일을 열기 (Open the zip files)
    for filename in filenames:
        if filename.endswith('.zip'):
            with zipfile.ZipFile(os.path.join(folder_path, filename), "r") as zip_obj:
                # zip 파일의 모든 파일을 압축 해제 (Extract all files from the zip file)
                zip_obj.extractall(folder_path)

def make_df(spark):
    # folder_path = "./data"
    # filenames = os.listdir(folder_path)    
    # for filename in filenames:
    #     if filename.endswith('.csv'):
    #         df = spark.read.csv(os.path.join(folder_path, filename), header=True)
    #         print(filename, df.count())
    schema = StructType([
        StructField('trip_id', IntegerType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('bikeid', IntegerType(), True),
        StructField('tripduration', StringType(), True),
        StructField('from_station_id', IntegerType(), True),
        StructField('from_station_name', StringType(), True),
        StructField('to_station_id', IntegerType(), True),
        StructField('to_station_name', StringType(), True),
        StructField('usertype', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('birthyear', IntegerType(), True)
    ])
    df = spark.read.csv('./data/Divvy_Trips_2019_Q4.csv', header=True, schema=schema)
    # Replace commas and cast the tripduration column to a numeric type
    df = df.withColumn("tripduration", regexp_replace(col("tripduration"), ",", "").cast("float"))
    return df


# What is the `average` trip duration per day?
def average_trip_duration_per_day(df):
    # Calculate the average trip duration per day
    directory_path="reports/average_trip_duration_per_day"
    remove_directory_if_exists(directory_path)
    # df = df.withColumn("start_time", from_unixtime(col("start_time"), "yyyy-MM-dd"))
    df = df.withColumn("start_date", to_date(df["start_time"]))
    avg_duration_per_day = df.groupBy("start_date").avg("tripduration").alias("avg_duration")
    # Coalesce the DataFrame to a single partition
    avg_duration_per_day = avg_duration_per_day.coalesce(1)
    avg_duration_per_day.write.csv(directory_path, header=True)

def trips_taken_each_day(df):
    directory_path="reports/trips_taken_each_day"
    remove_directory_if_exists(directory_path)
    # Calculate the number of trips taken each day
    df = df.withColumn("start_date", to_date(df["start_time"]))
    trips_per_day = df.groupBy("start_date").count().alias("trips_count")
    trips_per_day = trips_per_day.coalesce(1)
    trips_per_day.write.csv(directory_path, header=True)

def most_popular_starting_station_by_month(df):
    # Extract the month and year from the start_time
    directory_path="reports/most_popular_starting_station_by_month"
    remove_directory_if_exists(directory_path)

    df = df.withColumn("start_month", date_format("start_time", "yyyy-MM"))
    
    # Find the most popular starting station for each month
    station_counts = df.groupBy("start_month", "from_station_name").count()
    window_spec = Window.partitionBy("start_month").orderBy(station_counts["count"].desc())
    ranked_stations = station_counts.withColumn("rank", rank().over(window_spec)).filter("rank == 1")
    ranked_stations = ranked_stations.coalesce(1)
    ranked_stations.write.csv(directory_path, header=True)

def top_trip_stations_last_two_weeks(df):
    pass
    # Calculate the top 3 trip stations for the last two weeks
    # You can filter the data for the last two weeks using a date range condition
    # Then, count the occurrences of each station and select the top 3 for each day
    # Write the results to a CSV file

def compare_trip_duration_by_gender(df):
    directory_path="reports/trip_duration_by_gender"
    remove_directory_if_exists(directory_path)

    df = df.filter(col("gender").isNotNull())
    trip_duration_by_gender = df.groupBy("gender").avg("tripduration").alias("avg_duration")
    trip_duration_by_gender = trip_duration_by_gender.coalesce(1)
    trip_duration_by_gender.write.csv(directory_path, header=True)
    # Calculate the average trip duration for Male and Female users
    # You can group by the 'gender' column and calculate the average trip duration
    # Write the results to a CSV file

def top_10_longest_and_shortest_trip_ages(df):
    directory_path="reports/top_10_longest_and_shortest_trip_ages"
    remove_directory_if_exists(directory_path)
    
    # df 에서 birthyear null값 이면 제외
    df = df.filter(col("birthyear").isNotNull())
    
    # tripduration으로 데이터 정렬
    window_spec = Window.orderBy(df["tripduration"])

    # 상위 10개와 하위 10개 레코드 선택
    top_10 = df.select("tripduration", "birthyear").withColumn("rank", F.row_number().over(window_spec))
    max_rank = top_10.agg({"rank": "max"}).collect()[0][0]
    top_10 = top_10.filter((col("rank") <= 10) | (col("rank") >= max_rank - 9))
    top_10 = top_10.orderBy("rank")

    current_year = datetime.now().year
    top_10 = top_10.withColumn("age", current_year - col("birthyear"))
    top_10.write.csv(directory_path, header=True)
    # Calculate the top 10 ages of users with the longest and shortest trips
    # You can calculate age based on 'birthyear' and 'start_time'
    # Then, order by trip duration and select the top 10 and bottom 10
    # Write the results to a CSV file

def remove_directory_if_exists(directory_path):
    """
    주어진 디렉토리가 존재하면 제거하고, 없으면 다음 단계로 진행합니다.
    
    Args:
        directory_path (str): 삭제하려는 디렉토리의 경로.
    """
    if os.path.exists(directory_path):
        # 디렉토리가 존재하면 삭제
        shutil.rmtree(directory_path)
        print(f"{directory_path} 디렉토리를 제거했습니다.")
    else:
        # 디렉토리가 존재하지 않으면 메시지 출력
        print(f"{directory_path} 디렉토리가 존재하지 않습니다.")

def main():
    if os.path.isdir('reports'):
        pass
    else:
        os.mkdir('reports')
    spark = initialize_spark()
    unzip_data()
    df=make_df(spark)
    # df.printSchema()
    # df.show()
    average_trip_duration_per_day(df)
    trips_taken_each_day(df)
    most_popular_starting_station_by_month(df)
    # top_trip_stations_last_two_weeks(df)
    compare_trip_duration_by_gender(df)
    top_10_longest_and_shortest_trip_ages(df)

    spark.stop()


if __name__ == "__main__":
    main()
