import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
from plotly.offline import download_plotlyjs, plot
from plotly.graph_objs import *
import psycopg2

class MyClass:

def __init__(self):
    sparkSession = SparkSession.builder.master("local[*]") \
        .getOrCreate()


    host = self.servers
    conn = psycopg2.connect("host=host port='5432' dbname='wineDb' user='username' password='password'")
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
    url = "jdbc:postgresql://" + self.servers + "/wineDb?user=username&password=password"
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