from airflow.models.variable import Variable
from datetime import datetime 


user_db = Variable.get("user_db")
password_db = "password_db"
db_name = Variable.get('db_name')
port_db =  Variable.get('port_db') 
host_db = Variable.get('host_db') 
table_name = Variable.get('table_name') 
url = Variable.get('url') 
connection_string = f"postgresql://{user_db}:{user_db}@{host_db}:{port_db}/{db_name}"

default_config = {
    "table_name": table_name,
    "url": url,
    "connection_string": connection_string 
}

