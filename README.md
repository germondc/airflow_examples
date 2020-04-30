# airflow_examples

## Docker
Docker to start airflow container with port exposed:
```
sudo docker run -it --rm --entrypoint /bin/bash -p 8080:8080 apache/airflow
```

### Start up airflow
airflow initdb
airflow scheduler > /tmp/scheduler.log 2>&1 &
airflow webserver > /tmp/webserver.log 2>&1 &

### Put in DAGs
```
/opt/airflow/dags
```
