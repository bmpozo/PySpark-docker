# Run PySpark application in an Spark Standalone Cluster using Docker
Personal project to create an application using PySpark to run a quick analysis in a real data set.

# Contents

- [Requirements](#Requirements)
- [Tech Stack](#Tech-Stack")
- [App](#App)
- [Execution](#Execution)
- [Improvement](#Improvement)
- [Reference](#Reference)

# <a name="Requirements"></a>Requirements
## What it is required

* Docker version 20.10.12
* docker-compose version 1.25.0

# <a name="Tech-Stack"></a>Tech Stack
## Project Structure

The following project structure will be used

```sh
|
|--|src # Source directory for volume mounts(any code you want to deploy just paste it here)
|--|data # Data directory for volume mounts(any file you want to process just paste it here)
|--|get_lastfm_data.sh # startup script used to get the initial text file
|--|docker-compose.yml # the compose file

```

## The compose File

Now that we have our apache-spark image is time to create a cluster in docker-compose

```yaml
version: '3'
services:
  spark-master:
    image: bde2020/spark-master:3.2.0-hadoop3.2
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
      - "4040:4040"
    environment:
      - INIT_DAEMON_STEP=setup_spark
    volumes:
        - ./src:/opt/spark-src
        - ./data:/opt/spark-data
  spark-worker-1:
    image: bde2020/spark-worker:3.2.0-hadoop3.2
    container_name: spark-worker-1
    depends_on:
      - spark-master
    ports:
      - "8081:8081"
    environment:
      - "SPARK_MASTER=spark://spark-master:7077"
    volumes:
        - ./src:/opt/spark-src
        - ./data:/opt/spark-data
  spark-worker-2:
    image: bde2020/spark-worker:3.2.0-hadoop3.2
    container_name: spark-worker-2
    depends_on:
      - spark-master
    ports:
      - "8082:8081"
    environment:
      - "SPARK_MASTER=spark://spark-master:7077"
    volumes:
        - ./src:/opt/spark-src
        - ./data:/opt/spark-data
```

For both spark master and worker we mounted the following volumes:

Local folder|Docker container folder
---|---
scr|/opt/spark-src
data|/opt/spark-data

## Running Docker containers individually
### Spark Master
To start a Spark master:

    docker run --name spark-master -h spark-master -d bde2020/spark-master:3.2.0-hadoop3.2

### Spark Worker
To start a Spark worker:

    docker run --name spark-worker-1 --link spark-master:spark-master -d bde2020/spark-worker:3.2.0-hadoop3.2

## Running Docker containers cluster
In case there is any previous execution and the containers weren't deleted:

    docker rm -f $(docker ps -a -q)

To create the cluster run the compose file: 

    docker-compose up -d

To validate your cluster just access the spark UI on each worker & master URL

| Application     | URL                                      | Description                                                |
| --------------- | ---------------------------------------- | ---------------------------------------------------------- |
| Spark Driver    | [localhost:4040](http://localhost:4040/) | Spark Driver web ui                                        |
| Spark Master    | [localhost:8080](http://localhost:8080/) | Spark Master node                                          |
| Spark Worker I  | [localhost:8081](http://localhost:8081/) | Spark Worker node with 1 core and 512m of memory (default) |
| Spark Worker II | [localhost:8082](http://localhost:8082/) | Spark Worker node with 1 core and 512m of memory (default) |

# <a name="App"></a>App
## The Demo App

The following app can be found in src directory, this app is used as proof of concept of our cluster behavior.

## LastFM [Pyspark]

This program takes the archived data from [LastFM](http://web.mta.info/developers/MTA-Bus-Time-historical-data.html) and make some aggregations on it, the calculated results are persisted on files.

Each persisted file correspond to a particullar aggregation:re:

### Top 50 sessions

userid|group_session_id|number_songs|order_top_sessions
---|---|---|---
user_XXX|ABCD|100|1
user_YYY|ABCE|90|2

### Top 10 songs

track-name|number_reps|order_top_songs
---|---|---
Everlong|500|1
Money|200|2

To submit the app connect to your local environment:

    docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-src/lastfm/lastfm.py

OR

To submit the app connect to one of the workers or the master and execute:

    /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-src/lastfm/lastfm.py

# <a name="Execution"></a>Execution
## Step-by-step
1. Import the project
2. Run the bash script
```sh
sh get_lastfm_data.sh
```
3. Create docker cluster
```sh
docker-compose up -d
```
4. Run app in spark
```sh
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-src/lastfm/lastfm.py
```
5. Check output
```sh
cat data/output/data_top50_sessions.csv/part-* 
```
```sh
cat data/output/data_top10_songs.csv/part-*
```

# <a name="Improvement"></a>Improvement
## What's left to do?

* Input file to be uploaded into HDFS to run the app in cluster mode to have faster performance

* Write output in Hive or Hbase to not use coalesce(1) and enable a reporting tool (like PBI) to connect to explore the results

# <a name="Reference"></a>Reference
[big-data-europe](https://github.com/big-data-europe/docker-spark)