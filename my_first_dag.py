from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


send_telegram_message = TelegramOperator(
    task_id="send_telegram_message",
    token="<Токен бота в Telegram>",
    chat_id="Идентификатор чата, куда добавлен бот",
    text="Расчет сводки выполнен."
)

with DAG(
    "daily_effectiveness", # Идентификатор, отобразится в консоли
    default_args={
        "depends_on_past": False, # Зависимость задач от предыдущих
        "retries": 1, # Число перепопыток в случае неудаче
        "retry_delay": timedelta(seconds=30) # Интервал между попытками
    },
    description="Ежемесячная сводка маркетплейса", Описание, появится в консоли при наведении на название DAG
    schedule_interval='@monthly', # Ежемесячное исполнение
    start_date=datetime(2023, 7, 1), # Когда начать исполнение по расписанию
    catchup=False,
    tags=["Маркетплейс", "Dialogflow", "BigQuery"],
) as dag:

t1 = BashOperator(
     task_id="entering_virtual_environment", # Идентификатор таски для отслеживания в консоли
     bash_command="source /home/fitwist/airflow/airflow_env/bin/activate",
     retries=2 #
)

t2 = BashOperator(
      task_id="calculating_marketplace_effectiveness",
      depends_on_past=False,
 	bash_command="python3 /home/fitwist/airflow/df-to-looker/dialogflow-to-bigquery.py",
      retries=2
)

t1 >> t2 >> t3 >> t4 >> TelegramOperator(
    task_id="send_telegram_message",
    token="<telegram_bot_token>",
    chat_id="<chat_id>",
    trigger_rule=TriggerRule.ONE_FAILED,
    text="Группа чат-бота: одна из ежедневных выгрузок не выполнена. Проверь логи (Recent Tasks / Failed / Task Id / Log)."
)