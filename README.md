# Spark Recommendation Online and Offline

## system

install and start spark, hadoop and redis

## offline

```bash
spark-shell -i offline/als.scala
spark-shell -i offline/alsAndCos.scala
spark-shell -i offline/group_and_sort.scala 

# this only cache user id [1-100]
spark-shell -i offline/cache_offline.scala
```

## online

```bash
cd online
sbt package

spark-submit --class SimpleApp target/scala-2.12/simple-project_2.12-1.0.jar
```

## web

```bash
npm install

node socket/socket_server.js
node api/index.js

cd client
npm start
```