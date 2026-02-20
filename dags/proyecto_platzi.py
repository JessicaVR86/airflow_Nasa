from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def _generate_platzi_data(**kwargs):
    import pandas as pd
    data = pd.DataFrame({
        "student": ["Maria Cruz", "Daniel Crema", "Elon Musk", "Karol Castrejon", "Freddy Vega"],
        "timestamp": [kwargs['logical_date']] * 5
    })
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv", index=False)

def _notificar(**kwargs):
    print(f"Datos listos para el día {kwargs['ds']}")

with DAG(
    dag_id="platzi_explora_espacio",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Proyecto Platzi: recolectar datos del satélite y SpaceX"
) as dag:

    esperar_autorizacion = BashOperator(
        task_id="esperar_autorizacion",
        bash_command='sleep 20 && echo "OK" > /tmp/response_{{ ds_nodash }}.txt'
    )

    recolectar_platzi = PythonOperator(
        task_id="recolectar_platzi",
        python_callable=_generate_platzi_data
    )

    recolectar_spacex = BashOperator(
        task_id="recolectar_spacex",
        bash_command="curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history'"
    )

    notificar_equipos = PythonOperator(
        task_id="notificar_equipos",
        python_callable=_notificar
    )

    esperar_autorizacion >> recolectar_platzi >> recolectar_spacex >> notificar_equipos