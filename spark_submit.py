'''
Author:      venkatesh
Project:     stocks realtime analysis through spark structured streaming using docker
Date:        6/01/2023

'''


from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.types import *
from os import *
# from pyspark.conf import SparkConf
import psycopg2

spark = SparkSession.builder.appName("KafkaDataViewer").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "venkat_stream") \
    .load()

df1 = df.selectExpr("CAST(value AS STRING)")

df2 = df1.withColumn("Body", regexp_replace("value", "\"", ""))

df3 = df2.withColumn("actual", split(df2.Body, ",")) \
    .withColumn("Timestamp", to_timestamp(col("actual").getItem(0), "yyyy-MM-dd HH:mm:ss").cast("Timestamp")) \
    .withColumn("Bank_Name", col("actual").getItem(1)) \
    .withColumn("Open", col("actual").getItem(2).cast("decimal(38, 0)")) \
    .withColumn("High", col("actual").getItem(3).cast("decimal(38, 0)")) \
    .withColumn("Low", col("actual").getItem(4).cast("decimal(38, 0)")) \
    .withColumn("Close", col("actual").getItem(5).cast("decimal(38, 0)")) \
    .withColumn("Volume", col("actual").getItem(6).cast("decimal(38, 0)"))

df4 = df3.select("Timestamp", "Bank_Name", "Open", "High", "Low", "Close", "Volume")

df4.printSchema()


class AggInsertTimeDB:
    def process(self, row):
        StartTime = str(row.StartTime)
        EndTime = str(row.EndTime)
        Bank_Name = row.Bank_Name
        Open = row.Avg_OpenPrice
        High = row.Avg_HighPrice
        Low = row.Avg_LowPrice
        Close = row.Avg_ClosePrice
        Volume = row.Avg_Volume
        try:
            connection = psycopg2.connect(user="postgres",
                                          password="postgres",
                                          host="0.0.0.0",
                                          port="5432",
                                          database="demo_streaming")
            cursor = connection.cursor()

            sql_insert_query = """ INSERT INTO venkat_demo (StartTime, EndTime, Bank_Name, Avg_OpenPrice, Avg_HighPrice, Avg_LowPrice, Avg_ClosePrice, Avg_Volume) VALUES ('%s, '%s, '%s, '%d', '%d, '%d', '%d', '%d')""" % (StartTime, EndTime, Bank_Name, Open, High, Low, Close, Volume)

            print("\nsql_insert_query ", sql_insert_query)
            result = cursor.execute(sql_insert_query)
            result.commit()
            print(cursor.rowcount, "Record inserted successfully into venkat_demo table")
        except (Exception, psycopg2.Error) as error:
            print("Failed inserting record into table {}".format(error))
        finally:
            if connection:
                cursor.close()
                connection.close()


dfWindowed = df4.groupBy(window(df4.Timestamp, "5 seconds", "2 seconds"), df4.Bank_Name).mean().orderBy('window') \
    .select(col("window.start").alias("StartTime"), col("window.end").alias("EndTime"), "Bank_Name", col("avg(Open)").alias("Avg_OpenPrice"),
            col("avg(High)").alias("Avg_HighPrice"), col("avg(Low)").alias("Avg_LowPrice"),
            col("avg(Close)").alias("Avg_ClosePrice"), col("avg(Volume)").alias("Avg_Volume"))

print("dfWindowed schema")
dfWindowed.printSchema()

df4.writeStream.format("console").outputMode("append").option('truncate', 'false').start()
dfWindowed.writeStream.format("console").outputMode("complete").option('truncate', 'false').start().awaitTermination()

# ---> to run in yarn cluster
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --master yarn spark_submit.py

# ---> to run in spark standalone
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 spark_submit.py
