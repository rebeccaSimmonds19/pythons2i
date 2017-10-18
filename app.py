import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
import plotly.plotly as py
from plotly.offline import download_plotlyjs, init_notebook_mode, plot,iplot
init_notebook_mode(connected=True)

sparkSession = SparkSession.builder.master("local[*]") \
.config("spark.driver.extraClassPath","/opt/postgresql-42.1.4.jar") \
.getOrCreate()