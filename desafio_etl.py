from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import pandas as pd

# Definindo uma variável no Airflow
# Variable.set("my_email", "isadora.monteiro@indicium.tech")

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': "isadora.monteiro@indicium.tech",
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'NorthwindELT',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval=timedelta(days=1),  # Definindo a frequência da execução
    start_date=datetime(2024, 10, 10),
    catchup=False,
    tags=['ELT'],
) as dag:
    dag.doc_md = """
        ELT Diária do banco de dados de ecommerce Northwind,
        começando em 2022-02-07. 
    """

    # Função para ler dados da tabela Order e exportar para CSV
    def export_orders_to_csv():
        conn = sqlite3.connect('data/Northwind_small.sqlite')
        query = 'SELECT * FROM "Order";'
        df_orders = pd.read_sql_query(query, conn)
        conn.close()
        df_orders.to_csv('output_orders.csv', index=False)

    # Função para ler dados da tabela OrderDetail e calcular a quantidade vendida para o Rio de Janeiro
    def calculate_quantity_rio():
        conn = sqlite3.connect('data/Northwind_small.sqlite')
        query = 'SELECT * FROM "OrderDetail";'
        df_order_details = pd.read_sql_query(query, conn)
        df_orders = pd.read_csv('output_orders.csv')
       
       
        total_quantity = df_merged[df_merged['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
        
        with open('count.txt', 'w') as f:
            f.write(str(total_quantity))
        
        conn.close()

    # Função para criar o arquivo final_output.txt
    def export_final_output():
        with open('final_output.txt', 'w') as f:
            f.write("DAG executado com sucesso.")

    # Tarefas
    task1 = PythonOperator(
        task_id='export_orders_to_csv',
        python_callable=export_orders_to_csv,
        dag=dag,
    )

    task2 = PythonOperator(
        task_id='calculate_quantity_rio',
        python_callable=calculate_quantity_rio,
        dag=dag,
    )

    export_final_output_task = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_output,
        dag=dag,
    )

    # Ordenação das tarefas
    task1 >> task2 >> export_final_output_task

