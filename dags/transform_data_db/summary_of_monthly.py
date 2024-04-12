from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 10),
   
}

sql_query ="""
INSERT INTO summary_of_monthly
WITH I AS (
    SELECT
        order_id,
        SUM(amount) AS amount,
        SUM(profit) AS profit,
        SUM(quantity) AS quantity
    FROM
        public.order_details
    GROUP BY
        order_id
),
II AS (
    SELECT
        year,
        month,
        a.order_id,
        amount,
        profit,
        quantity
    FROM
        I a
    LEFT JOIN (
        SELECT
            order_id ,
            EXTRACT(YEAR FROM TO_DATE(order_date, 'DD-MM-YYYY')) AS year,
            EXTRACT(MONTH FROM TO_DATE(order_date, 'DD-MM-YYYY')) AS month
        FROM
            public.list_of_orders
    ) b ON a.order_id = b.order_id
)
SELECT
    year,
    month,
    SUM(amount) AS amount,
    SUM(profit) AS profit,
    SUM(quantity) AS quantity
FROM
    II
GROUP BY
    year,
    month
ORDER BY
    year DESC,
    month ASC
"""
with DAG(
    'summary_of_monthly',
    default_args=default_args,
    description='A DAG to execute SQL queries using PostgreSQLOperator',
    schedule_interval=None,  # Set the schedule interval as needed
) as dag:
    execute_sql_task = PostgresOperator(
        task_id='execute_sql_task',
        postgres_conn_id='postgres_conn',  
        sql=sql_query,
    )
