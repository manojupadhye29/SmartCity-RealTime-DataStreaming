from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from config import configuration

# spark = SparkSession.builder.appName('smart_city') \
#     .config('spark.jars.packages',
#             'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,'
#             'org.apache.hadoop:hadoop-aws:3.3.1,'
#             'com.amazonaws:aws-java-sdk:1.11.469,') \
#     .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
#     .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY')) \
#     .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_SECRET_KEY')) \
#     .getOrCreate()

spark = SparkSession.builder.appName('smart_city') \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1') \
    .getOrCreate()

# adjust the log level to minimize the console output on the executors
spark.sparkContext.setLogLevel('WARN')

# vehicle schema
vehicle_schema = StructType([
    StructField('id', StringType(), True),
    StructField('device_id', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('location', StringType(), True),
    StructField('speed', DoubleType(), True),
    StructField('direction', StringType(), True),
    StructField('make', StringType(), True),
    StructField('model', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('fuel_type', StringType(), True)
])

# gps schema
gps_schema = StructType([
    StructField('id', StringType(), True),
    StructField('device_id', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('direction', StringType(), True),
    StructField('vehicle_type', StringType(), True)
])

# traffic schema
traffic_schema = StructType([
    StructField('id', StringType(), True),
    StructField('device_id', StringType(), True),
    StructField('camera_id', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('snapshot', StringType(), True)
])

# weather schema
weather_schema = StructType([
    StructField('id', StringType(), True),
    StructField('device_id', StringType(), True),
    StructField('location', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('temperature', DoubleType(), True),
    StructField('weather_condition', StringType(), True),
    StructField('precipitation', DoubleType(), True),
    StructField('wind_speed', DoubleType(), True),
    StructField('humidity', IntegerType(), True),
    StructField('AQI', DoubleType(), True)
])

# emergency schema
emergency_schema = StructType([
    StructField('id', StringType(), True),
    StructField('incident_id', StringType(), True),
    StructField('type', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('location', DoubleType(), True),
    StructField('status', StringType(), True),
    StructField('description', StringType(), True)
])


def read_kafka_topic(topic, schema):
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', 'topic')
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(values AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', ' 2 minutes')
            )


def streamWriter(input: DataFrame, checkpoint_folder, output):
    return (input.writeStream
            .format('parquet')
            .option('checkpointLocation', 'checkpointFolder')
            .option('path', output)
            .outputMode('append')
            .start())


vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
gps_df = read_kafka_topic('gps_data', gps_schema).alias('gps')
traffic_df = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
weather_df = read_kafka_topic('weather_data', weather_schema).alias('weather')
emergency_df = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

streamWriter(vehicle_df, '../output/checkpoint/vehicle_data', '../output/vehicle_data')
streamWriter(gps_df, '../output/checkpoint/gps_data', '../output/gps_data')
streamWriter(traffic_df, '../output/checkpoint/traffic_data', '../output/traffic_data')
streamWriter(weather_df, '../output/checkpoint/weather_data', '../weather_data')
streamWriter(emergency_df, '../output/checkpoint/emergency_data', '../emergency_data')

