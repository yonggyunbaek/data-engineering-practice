# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, hash
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
# import pandas as pd
# from io import StringIO

# def make_pddf():
#     # pddf = pd.read_csv(r'./data/hard-drive-2022-01-01-failures.csv.zip', compression='zip', sep=',')
#     zipfile_path = './data/hard-drive-2022-01-01-failures.csv.zip'

#     with zipfile.ZipFile(zipfile_path, 'r') as zip_file:
#         select_file = 'hard-drive-2022-01-01-failures.csv.csv'

# 1. Add the file name as a column to the DataFrame

def make_df(spark, zip_file_path, select_file):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        with zip_file.open(select_file) as selected_file:
            # print(selected_file)
            # <zipfile.ZipExtFile name='hard-drive-2022-01-01-failures.csv' mode='r' compress_type=deflate>
            # sparkdf = spark.createDataFrame(selected_file.name(), header=True, inferSchema=True)
            
            csv_content = selected_file.read().decode('utf-8')
            csv_lines = csv_content.strip().split("\n")
            
            # 첫 번째 줄을 스키마로 사용
            schema = csv_lines[0].split(",")
            # 스키마와 데이터 행을 분리
            data_lines = csv_lines[1:]
            
            csv_rows = [Row(*line.split(",")) for line in data_lines]
            df = spark.createDataFrame(csv_rows, schema)
    return df

# 1. Add the file name as a column to the DataFrame
def add_source_file_column(df, file_name):
    df_with_source_file = df.withColumn("source_file", col(lit(file_name)))
    return df_with_source_file

# 2. Extract the date from the source_file column and cast it to timestamp
def extract_and_cast_date(df):
    df_with_date = df.withColumn("file_date", col("source_file").substr(-10, 10).cast(TimestampType()))
    return df_with_date

# 3. Extract the brand from the model column
def extract_brand(df):
    df_with_brand = df.withColumn("brand", when(col("model").contains(" "), split(col("model"), " ")[0]).otherwise("unknown"))
    return df_with_brand

# 4. Create storage ranking based on capacity_bytes
def create_storage_ranking(df):
    window_spec = Window.orderBy(col("capacity_bytes").desc())
    df_with_ranking = df.withColumn("storage_ranking", rank().over(window_spec))
    return df_with_ranking

# 5. Create a primary_key column as a hash of unique columns
def create_primary_key(df):
    df_with_primary_key = df.withColumn("primary_key", hash(col("column1"), col("column2"), ...))  # Add all unique columns here
    return df_with_primary_key



def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    # your code here
    zip_file_path = './data/hard-drive-2022-01-01-failures.csv.zip'
    file_name = 'hard-drive-2022-01-01-failures.csv'

    df = make_df(spark, zip_file_path, file_name)
    df = add_source_file_column(df, file_name)
    df['source_file'].show()






if __name__ == "__main__":
    main()
