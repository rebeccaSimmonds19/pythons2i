import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc

sparkSession = SparkSession.builder.master("spark://test:7077") \
.getOrCreate()