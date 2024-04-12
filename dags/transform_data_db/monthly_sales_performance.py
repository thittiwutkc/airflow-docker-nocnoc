from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 10),
}

sql_query = """
INSERT INTO monthly_sales_performance
WITH I AS (
    SELECT
        EXTRACT(YEAR FROM TO_DATE(CONCAT('01-', month_of_order_date), 'DD-Mon-YY')) AS year,
        EXTRACT(MONTH FROM TO_DATE(CONCAT('01-', month_of_order_date), 'DD-Mon-YY')) AS month,
        category AS categories,
        target
    FROM
        public.sales_target
),
II AS (
    SELECT
        year,
        month,
        categories,
        SUM(target) AS target_amount
    FROM
        I
    GROUP BY
        year, month, categories
),
III AS (
    SELECT
        EXTRACT(YEAR FROM TO_DATE(loo.order_date , 'DD-MM-YYYY')) AS year,
        EXTRACT(MONTH FROM TO_DATE(loo.order_date, 'DD-MM-YYYY')) AS month,
        category,
        SUM(amount) AS actual_amount
    FROM
        order_details od
    LEFT JOIN
        list_of_orders loo ON od.order_id = loo.order_id
    GROUP BY
        year, month, category
)
SELECT
    II.year,
    II.month,
    II.categories,
    II.target_amount,
    III.actual_amount,
    CAST((III.actual_amount / II.target_amount) * 100.0 AS DECIMAL(10, 2)) AS percent_achieve
FROM
    II
LEFT JOIN
    III ON II.year = III.year AND II.month = III.month AND II.categories = III.category
ORDER BY
    year DESC,
    month ASC
"""

with DAG(
    'monthly_sales_performance',
    default_args=default_args,
    description='A DAG to execute SQL queries using PostgreSQLOperator',
    schedule_interval=None,  
) as dag:
    execute_sql_task = PostgresOperator(
        task_id='execute_sql_task',
        postgres_conn_id='postgres_conn', 
        sql=sql_query,
    )