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


@app.route('/map')                                            
def index(choromap):
    #get the html file path
    plot_url = plot(choromap, filename='map.html')
    print(plot_url)
    #make the templates dir
    newpath = r'/templates' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    #move the file to the templates dir
    os.rename('opt/root-app/src/map.html, 'templates/map.html')
    resp = render_template("map.html", title = 'Maps')
    return resp

import psycopg2
conn = psycopg2.connect("host='172.17.0.3' port='5432' dbname='wineDb' user='username' password='password'")
cur = conn.cursor()
#make table
f = open(r'/opt/app-root/src/wineData.csv', 'r')
cur.copy_from(f, "wine_reviews", sep=',')
conn.commit()
#cur.execute('create table wine_reviews(country VARCHAR, designation VARCHAR, points INT, price VARCHAR, province VARCHAR, region_1 VARCHAR, region_2 VARCHAR, variety VARCHAR, winery VARCHAR);')
#conn.commit()
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


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
