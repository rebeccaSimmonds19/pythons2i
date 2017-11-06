from flask import Flask
from flask import app, render_template
import os
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
from plotly.offline import download_plotlyjs, plot
from plotly.graph_objs import *
import psycopg2
import argparse


class MyClass:

    def __init__(self):
        parser = argparse.ArgumentParser(description='map')
        parser.add_argument('--servers', help='the postgreql ip address')
        args = parser.parse_args()

        self.make(self,args.servers)

    def make(self, servers):
        sparkSession = SparkSession.builder.master("local[*]") \
            .getOrCreate()
        conn = psycopg2.connect("host="+servers+"port='5432' dbname='wineDb' user='username' password='password'")
        cur = conn.cursor()

        # does table exist
        tb_exists = "select exists(select relname from pg_class where relname='" + "wine_reviews" + "')"
        cur.execute(tb_exists)
        execute = cur.fetchone()[0]
        if not execute:
            # mak)e table
            cur.execute(
                'create table wine_reviews(country VARCHAR, designation VARCHAR, points INT, price VARCHAR, province VARCHAR, region_1 VARCHAR, region_2 VARCHAR, variety VARCHAR, winery VARCHAR);')
            conn.commit()
        # copy csv
        f = open(r'/opt/app-root/src/wineData.csv', 'r')
        cur.copy_from(f, "wine_reviews", sep=',')
        conn.commit()
        f.close()
        url = "jdbc:postgresql://" + servers + "/wineDb?user=username&password=password"
        df = (sparkSession.read.format("jdbc")
              .options(url=url, dbtable="wine_reviews")
              .load())
        table = df.select('country', 'points').groupBy('country').agg(mean('points')).orderBy('avg(points)',
                                                                                              ascending=False)
        countryCols = table.select('country').collect()
        countries = list()
        for country in countryCols:
            countries.append(str(country[0]))
        pointCols = table.select('avg(points)').collect()
        points = list()
        for point in pointCols:
            points.append(point[0])
        data = dict(type='choropleth',
                    locationmode='country names',
                    locations=countries,
                    colorscale='Blues',
                    z=points,
                    colorbar={'title': 'Average Rating'}
                    )
        layout = dict(geo={'scope': 'world'})
        choromap = dict(data=[data], layout=layout)
        # get the html file path
        plot(choromap, filename='map.html')

def setUp():
    MyClass()

app = Flask(__name__)

@app.route('/')
def index():
    setUp()
     # make the templates dir
    newpath = r'/opt/app-root/src/templates'
    if not os.path.exists(newpath):
        os.makedirs(newpath, 0o77)
    # move the file to the templates dir
    os.rename('/opt/app-root/src/map.html', '/opt/app-root/src/templates/map.html')
    resp = render_template("map.html", title='Maps')
    return resp

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)