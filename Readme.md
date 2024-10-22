CREATE TABLE news_articles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(255),
    source_name VARCHAR(255),
    author VARCHAR(255),
    title TEXT,
    domain VARCHAR(255),
    description TEXT,
    url TEXT,
    url_to_image TEXT,
    published_at DATETIME,
    content TEXT,
    lda_topics JSON  -- This field stores LDA results as a JSON string
);

docker pull apache/airflow:2.10.2-python3.8

docker pull adminer

docker pull mysql:8.0

docker exec -it etlairflow-airflow-webserver-1 airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@example.com --password admin


adminer

System: MySQL
Server: mysql
Username: airflow
Password: airflow
Database: airflow_db


Conn Id: mysql_conn (or any connection ID you want to reference in your code).
Conn Type: MySQL.
Host: mysql (or localhost if you're running MySQL locally, but in Docker it is likely mysql).
Schema: airflow_db (the name of the database you're using).
Login: airflow (the username you defined in the Docker setup).
Password: airflow (the corresponding password for the user).
Port: 3306 (default MySQL port).
