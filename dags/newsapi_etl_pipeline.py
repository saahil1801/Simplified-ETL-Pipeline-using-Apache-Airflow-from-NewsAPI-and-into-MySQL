from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import dag, task
from newsapi import NewsApiClient
from datetime import datetime, timedelta
import dateutil.parser
import os , json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG using the @dag decorator
@dag(
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Daily schedule
    catchup=False,
    description='Simplified ETL pipeline using NewsAPI to fetch business news and store in MySQL',
    dag_id='simplified_newsapi_etl_pipeline',
)
def newsapi_etl_pipeline():

    # Task to extract data from NewsAPI
    @task
    def extract_news():
        api_key = "db1bab7b36c84cd8a0ed318d45a27972"  # Replace with your API Key
        newsapi = NewsApiClient(api_key=api_key)
        top_headlines = newsapi.get_top_headlines(country='us', category='business')
        return top_headlines['articles']

    # Task to transform the extracted data
    @task
    def transform_news(articles):
        transformed_articles = []
        for article in articles:
            # Parse the 'published_at' field into MySQL-compatible datetime format
            if article.get('publishedAt'):
                published_at = dateutil.parser.parse(article['publishedAt']).strftime('%Y-%m-%d %H:%M:%S')
            else:
                published_at = None

            # Handling missing values
            author = article.get('author') if article.get('author') else 'Unknown'
            description = article.get('description') if article.get('description') else ''

            # Clean and prepare the title
            title = article.get('title', '').lower().strip()

            # Extract the domain from URL
            domain = article.get('url').split('/')[2] if article.get('url') else ''

            # Create the transformed article structure
            transformed_article = {
                'source_id': article['source'].get('id'),
                'source_name': article['source']['name'],
                'author': author,
                'title': title,
                'domain': domain,  # Extract domain from URL
                'description': description,
                'url': article.get('url'),
                'url_to_image': article.get('urlToImage'),
                'published_at': published_at,  # Converted datetime
            }
            transformed_articles.append(transformed_article)

        
        data_dir = '/opt/airflow/data'
        os.makedirs(data_dir, exist_ok=True)  # Ensure the directory exists
        file_path = os.path.join(data_dir, 'transformed_articles.json')

        # Save the transformed articles to a JSON file
        with open(file_path, 'w') as f:
            json.dump(transformed_articles, f)

        return transformed_articles

    # Task to load the transformed data into MySQL
    @task
    def load_news(transformed_articles):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')  # Set up this connection in Airflow UI
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()

        # MySQL insert statement
        insert_stmt = """
            INSERT INTO news_articles (source_id, source_name, author, title, domain, description, url, url_to_image, published_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Insert each transformed article into the MySQL database
        for article in transformed_articles:
            cursor.execute(insert_stmt, (
                article['source_id'],
                article['source_name'],
                article['author'],
                article['title'],
                article['domain'],
                article['description'],
                article['url'],
                article['url_to_image'],
                article['published_at'],
            ))
        connection.commit()
        cursor.close()

    # Task dependencies
    articles = extract_news()
    transformed_articles = transform_news(articles)
    load_news(transformed_articles)

# Instantiate the DAG
newsapi_etl_pipeline()

