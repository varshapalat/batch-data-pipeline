FROM sequenceiq/hadoop-docker:2.7.0


RUN /etc/bootstrap.sh \
    && sleep 60 \
    && /usr/local/hadoop/bin/hadoop fs -mkdir -p /user/root/datalake/ingest \
     && /usr/local/hadoop/bin/hadoop fs -mkdir -p /user/root/datalake/transform \
     && /usr/local/hadoop/bin/hadoop fs -chown -R airflow /user

