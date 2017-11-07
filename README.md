# pythons2i
This repository shows a pyspark s2i connected to a postgresql db to deliver a wine review heat map from plotly

This app is all run on openshift using oc cluster up

1) start the cluster

2) Using a docker image pull a postgresql image:

docker pull registry.access.redhat.com/openshift3/postgresql-92-rhel7

3) Then create a postgresql app to use this:

oc new-app \
    -e POSTGRESQL_USER=username \
    -e POSTGRESQL_PASSWORD=password \
    -e POSTGRESQL_DATABASE=wineDb \
    registry.access.redhat.com/openshift3/postgresql-92-rhel7:latest
    
4) Make sure you have the right resources:

oc create -f https://radanalytics.io/resources.yaml

5) Create the wine map app

oc new-app --template oshinko-pyspark-build-dc \      
-p APPLICATION_NAME=pythons2i    \
-p GIT_URI=https://github.com/rebeccaSimmonds19/pythons2i.git \
-p SPARK_OPTIONS='--jars postgresql-42.1.4.jar --conf spark.driver.extraClassPath=postgresql-42.1.4.jar \
-p APP_ARGS=--servers='172.17.0.3'

6) Create a route for the app and then go to the link.
