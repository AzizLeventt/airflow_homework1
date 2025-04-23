from datetime import datetime
import random

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import psycopg2

MONGO_URI = ("mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
client = MongoClient(MONGO_URI, server_api=ServerApi("1"))


DB_NAME = "bigdata_training"
SRC_COLL = "sample_coll"                         
ANOMALIES_COLL = "anomalies_azizmutlulevent"
LOG_COLL = "log_azizmutlulevent"

class HeatAndHumidityMeasureEvent:
    def __init__(self, temperature: int, humidity: int, timestamp: datetime, creator: str):
        self.temperature = temperature
        self.humidity = humidity
        self.timestamp = timestamp
        self.creator = creator



def generate_random_heat_and_humidity_data(record_count: int = 10):
    records = []
    for _ in range(record_count):
        temp = random.randint(10, 40)
        humid = random.randint(10, 100)
        ts = datetime.utcnow()
        records.append(HeatAndHumidityMeasureEvent(temp, humid, ts, "airflow"))
    return records


def save_data_to_mongodb(records):
    db = client[DB_NAME]
    coll = db[SRC_COLL]
    coll.insert_many([r.__dict__ for r in records])


def create_sample_data_on_mongodb(**_):
    save_data_to_mongodb(generate_random_heat_and_humidity_data(10))


def copy_anomalies_into_new_collection(**_):
    db = client[DB_NAME]
   
    if ANOMALIES_COLL not in db.list_collection_names():
        db.create_collection(ANOMALIES_COLL)

    src = db[SRC_COLL]
    dest = db[ANOMALIES_COLL]
    anomalies = src.find({"temperature": {"$gt": 30}})
    for doc in anomalies:
        doc.pop("_id", None)
        doc["creator"] = "azizmutlulevent"
        dest.insert_one(doc)


def copy_airflow_logs_into_new_collection(**_):
    conn = psycopg2.connect(
        host="postgres", port=5432, database="airflow",
        user="airflow", password="airflow"
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event, COUNT(*)
        FROM log
        WHERE dttm >= NOW() - INTERVAL '1 minute'
        GROUP BY event;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    db = client[DB_NAME]
    log_coll = db[LOG_COLL]
    now = datetime.utcnow()
    for event, count in rows:
        log_coll.insert_one({
            "event_name": event,
            "record_count": count,
            "created_at": now,
        })


with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="*/1 * * * *",
    default_args={"retries": 0},
) as dag:
    start = DummyOperator(task_id="start")

    generate_and_save = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data_on_mongodb,
    )

    copy_anomalies = PythonOperator(
        task_id="copy_anomalies",
        python_callable=copy_anomalies_into_new_collection,
    )

    aggregate_logs = PythonOperator(
        task_id="insert_airflow_logs",
        python_callable=copy_airflow_logs_into_new_collection,
    )

    finaltask = DummyOperator(task_id="finaltask")

    
    start >> generate_and_save >> copy_anomalies >> finaltask
    start >> aggregate_logs >> finaltask
