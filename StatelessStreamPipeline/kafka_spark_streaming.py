from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

##app-name
### → run locally using 2 cores (threads)
## ensures that Spark finishes processing the current micro-batch before shutting down, preventing data loss.
## jar file already in the gcloud cluster

spark=SparkSession.builder \
.appName('KafkaReader') \
.master('local[2]') \
.config('spark.streaming.stopGracefullyOnShutdown','true') \
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
.getOrCreate()

## Define your schema 
schema = StructType([
    StructType("id",IntegerType()),
    StructField("name",StringType()),
    StructField("age",IntegerType())
])

## read from kafka 
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "user_data") \
  .option("startingOffsets", "latest") \
  .load()

## convert the data and load 
# selectExpr("CAST(value AS STRING)")
# Converts Kafka binary value to string
# 2️⃣
# from_json(..., schema)
# Parses JSON string into structured columns
# 3️⃣
# select("data.*")
# Expands the struct into individual columns
# 4️⃣
# filter(col("age") > 25)
# Keeps only rows with age > 25

data=df.selectExpr("CAST (value AS STRING)") \
.select(from_json(col("value").cast("string"),schema).alias("data")) \
.select("data.*") \
.filter(col("age")>25)

## start streaming and print to console 
query=data.writeStream \
.outputMode('update') \
.format('console').start()

query.awaitTermination()




