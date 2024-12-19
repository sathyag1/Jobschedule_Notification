"##### Import necessary library for accessing Elasticsearch source"
                           
import psycopg2
from io import StringIO
import os
from dotenv import load_dotenv

load_dotenv()

PG_USER = os.getenv('USER')
PG_PASSWORD = os.getenv('PASSWORD')
PG_HOST = os.getenv('HOST')
PG_PORT = os.getenv('PORT')
PG_DATABASE = os.getenv('DATABASE')

try:
    connection = psycopg2.connect(user = PG_USER, 
                                password = PG_PASSWORD,
                                host = PG_HOST, 
                                port = PG_PORT, 
                                database = PG_DATABASE)  

    cur = connection.cursor()
    cur.execute("CALL public.job_master_incremental_load();")
    print("job_master_incremental_load procudure is executed successfully")
    connection.commit()
    cur.close()
    connection.close()
except (Exception, psycopg2.DatabaseError) as error:
        print(error)
finally:
    if connection is not None:
        connection.close()   
