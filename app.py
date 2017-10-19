import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
import plotly.plotly as py

sparkSession = SparkSession.builder.master("local[*]") \
.config("spark.driver.extraClassPath","opt/app-root/src/postgresql-42.1.4.jar") /
.getOrCreate()

#sparkSession.sparkContext.addJar("postgresql-42.1.4.jar")

#._jsc.addJar("postgresql-42.1.4.jar") \
#.config("spark.driver.extraClassPath","postgresql-42.1.4.jar") \
#.config("spark.jars", "postgresql-42.1.4.jar") \
#sparkSession.sparkContext.addFile("postgresql-42.1.4.jar")

import psycopg2
conn = psycopg2.connect("host='172.17.0.3' port='5432' dbname='wineDb' user='username' password='password'")
cur = conn.cursor()
#make table
f = open(r'/opt/app-root/src/wineData.csv', 'r')
cur.copy_from(f, "wine_reviews", sep=',')
conn.commit()
f.close()

import plotly.graph_objs as go

url = "jdbc:postgresql://172.17.0.3/wineDb?user=username&password=password"
df = (sparkSession.read.format("jdbc")
    .options(url=url, dbtable="wine_reviews")
    .load())

table = df.select('country','points').groupBy('country').agg(mean('points')).orderBy('avg(points)',ascending=False)
countryCols = table.select('country').collect()
countries = list()
for country in countryCols:
    countries.append(str(country[0]))
pointCols = table.select('avg(points)').collect()
points = list()
for point in pointCols:
    points.append(point[0])
data =  dict(type = 'choropleth',
        locationmode='country names',
        locations = countries,
        colorscale='Blues',
        z = points,
        colorbar = {'title': 'Average Rating'}
)
layout = dict(geo = {'scope':'world'})
choromap = go.Figure(data = [data],layout = layout)
py(choromap)