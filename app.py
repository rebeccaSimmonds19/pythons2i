import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc

spark = SparkSession.builder.appName("wine-map").getOrCreate()
