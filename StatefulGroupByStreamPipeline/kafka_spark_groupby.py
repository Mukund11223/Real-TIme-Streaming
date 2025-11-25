from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

## defining the sparksession
spark= SparkSession.builder \
    .appName('GroupByStream') \
    .master('local[3]') \
    .config('spark.streaming.stopGracefullyOnShutdown','true') \
    .config('spark.sql.shuffle.partitions','2') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

## reading the data from kafka 
df=spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers','localhost:9092') \
    .option('subscribe','trx_data') \
    .option('startingoffsets','earliest') \
    .load()

##defining the schema 
schema=StructType([
    StructField('user_id',StringType()),
    StructField('amount',IntegerType()),
    StructField('timestamp',TimestampType())
])

#parse the json data
df=df.select(from_json(col('value').cast('string'),schema).alias("data")).select("data.*")

##aggregation 
df=df.groupBy('user_id').agg(sum(col('amount')).alias('total_amount'))

## writing the output 
query=df.writeStream \
    .format('console') \
    .outputMode('complete') \
    .start()

## wait for the query to terminate 
query.awaitTermination()


