FROM apache/airflow:2.10.2-python3.8

USER airflow
RUN pip3 install --no-cache-dir \
    pymysql \
    newsapi-python \
    pyspark 
   

