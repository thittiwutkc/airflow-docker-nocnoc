from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 10),
}

sql_query = """
INSERT INTO top_3_customer 
WITH CustomerTotalAmount AS (
    SELECT
        loo.city AS city,
        loo.customername AS customername,
        SUM(od.amount) AS amount
    FROM
        order_details od 
    LEFT JOIN
        (SELECT * FROM list_of_orders WHERE EXTRACT(YEAR FROM TO_DATE(order_date, 'DD-MM-YYYY')) = '2018') loo 
        ON od.order_id = loo.order_id 
    GROUP BY
        loo.city, loo.customername
),
RankedCustomers AS (
    SELECT
        city,
        customername AS customer_name,
        amount,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY SUM(amount) DESC) AS rank
    FROM
        CustomerTotalAmount
    GROUP BY
        city, amount, customername
)
SELECT
    city,
    customer_name,
    amount
FROM
    RankedCustomers
WHERE
    rank <= 3
"""

with DAG(
    'top_3_customer',
    default_args=default_args,
    description='A DAG to execute SQL queries using PostgreSQLOperator',
    schedule_interval=None, 
) as dag:
    
    execute_sql_task = PostgresOperator(
        task_id='execute_sql_task',
        postgres_conn_id='postgres_conn', 
        sql=sql_query,
    )
