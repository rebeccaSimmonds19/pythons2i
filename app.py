import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
from plotly.offline import download_plotlyjs, plot
from plotly.graph_objs import *
from flask import Flask
from flask import request
from flask import app, make_response, render_template
import os


sparkSession = SparkSession.builder.master("local[*]") \
.getOrCreate()

app = Flask(__name__)


@app.route('/')                                            
def index(choromap):
    plot_url = plot(choromap)
    print(plot_url)
    #resp = make_response(render_template(plot_url))
    return plot_url

import psycopg2
conn = psycopg2.connect("host='172.17.0.3' port='5432' dbname='wineDb' user='username' password='password'")
cur = conn.cursor()
#make table
f = open(r'/opt/app-root/src/wineData.csv', 'r')
cur.copy_from(f, "wine_reviews", sep=',')
conn.commit()
f.close()

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
choromap = dict(data=[data], layout=layout)
index(choromap)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)


