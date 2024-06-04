# Ingest_Flink_Job 
* A stream processing flink job that ingest data from a kafka topic and output into a postgresql db  

## Setup
```bash
## Requirements
# Download the apache flink and setup 
https://flink.apache.org/downloads/

# Download and setup the kafka broker 
https://kafka.apache.org/downloads

# Create a kafka source 
# Use this kafka source or any other source 
https://github.com/BarunW/go-kafka-producer

# For output use a postgres 
# Here we use postgres docker container
docker pull postgres:latest
docker run --name <container_name> -e POSTGRES_PASSWORD=<password> -d -p 5432:5432 -v <volume_path> postgres
# <volume_path_linux> = pg_data:/var/lib/postgresql/data
# establish a psql shell
psql -h localhost -p 5432 -U postgres 
# create a database that should be link with jdbc
CREATE DATABASE user_interaction_data 
```
## Starting a Flink Job
```
# build the project
cd <repo>
mvn clean package

# start the flink 
<path_to_flink>/bin/start-cluster.sh

# register the flink job 
<path_to_flin>/bin/flink run -c consumer.App target/IngestorJob-1.0-SNAPSHOT.jar

# access from the web ui 
http://localhost:8081/
```