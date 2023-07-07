''' Cкрипт позволяет выгружать логи Dialogflow, выполнять SQL-запрос, подсчитывающий число реплик за день, 
и загружать в другую таблицу BigQuery результат.'''

from google.cloud import bigquery
import os

os.environ["GCLOUD_PROJECT"] = "<projectID>"

client = bigquery.Client()

sql = """
    SELECT 
      TIMESTAMP_TRUNC(Timestamp, day) Timestamp,
      COUNT(1) `Count`
FROM `<projectID>.<datasetId>.dialogflow_agent_*`
GROUP BY 1
ORDER BY Timestamp
"""

sessions = client.query(sql).to_dataframe()

dataframe = pd.DataFrame(
    sessions,
    columns=["Timestamp", "Count"],
)

# print(dataframe)
import_table_id = "<projectID>.<datasetId>.Реплик за день"

job_config = bigquery.LoadJobConfig(autodetect=True)

# Выполним присоединение записи
job = client.load_table_from_dataframe(
    dataframe, import_table_id, job_config=job_config
)
job.result()