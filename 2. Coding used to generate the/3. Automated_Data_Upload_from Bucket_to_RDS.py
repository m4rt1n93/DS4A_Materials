########################################################################################################################
## Import libraries

import boto3
import pandas as pd
import pandas_schema
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
import numpy as np
from decimal import *
from sqlalchemy import create_engine
import re
import io
from sqlalchemy import text
import time

import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.max_columns', None)

########################################################################################################################
## Download s3 Data

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='AKIATARFV5X3IF6XPCVC',
    aws_secret_access_key='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'
)

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

# Download file and read from disc
def download_history():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id='AKIATARFV5X3IF6XPCVC',
        aws_secret_access_key='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'
    )
    try:
        s3.Bucket('teate-ds4a-team77').download_file(Key='historico/historico.csv', Filename='historico2.csv')
        historico = pd.read_csv('historico2.csv')
        s3.Bucket('teate-ds4a-team77').download_file(Key='nueva_data/nueva_data.csv', Filename='nueva_data2.csv')
        nueva_data = pd.read_csv('nueva_data2.csv')
        print('Succesful charge')
        charge = True
    except:
        historico = None
        nueva_data = None
        charge = False
        print('Not new files')
    return historico, nueva_data, charge

########################################################################################################################
## Error validation

def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True
def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True

decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

schema = pandas_schema.Schema([
            Column('cantidaddepedido', int_validation + null_validation),
            Column('year', decimal_validation),
            Column('index', int_validation + null_validation), Column('pedido', int_validation + null_validation), Column('fechapedido', null_validation), Column('tienda', int_validation + null_validation), Column('nombretienda', null_validation),
            Column('direcciontienda', null_validation), Column('fabricante', int_validation + null_validation), Column('nombrefabricante', null_validation), Column('material', int_validation + null_validation),
            Column('nombrematerial', null_validation), Column('um', null_validation), Column('valorunitariopedido', int_validation + null_validation),
            Column('valortotalpedido', int_validation + null_validation), Column('entrega'), Column('factura'), Column('zona'), Column('nombrezonacomercial'),
            Column('ruta'), Column('nombreruta'), Column('comuna'), Column('barrio', null_validation), Column('poblacion', null_validation), Column('row_number', int_validation + null_validation),
            Column('month', int_validation + null_validation), Column('day', int_validation + null_validation)
])

# apply validation
def data_validation(nueva_data):
    errors = schema.validate(nueva_data)
    errors_index_rows = [e.row for e in errors]
    if len(errors_index_rows) > 0:
        validation = False
    else:
        validation = True
    return errors, errors_index_rows, validation

########################################################################################################################
## Unification

def unification(historico, nueva_data):
    #Create DF final and drop duplicates
    df_final = pd.concat([nueva_data, historico])
    df_final = df_final.drop_duplicates(subset=['pedido', 'material'], keep='first')
    df_final.to_csv('df_final.csv', index=False)

    #Upload historico
    s3.Bucket('teate-ds4a-team77').upload_file(Filename='df_final.csv', Key='historico/historico.csv')

    #Drop new file
    s3.Object('teate-ds4a-team77', 'nueva_data/nueva_data.csv').delete()


########################################################################################################################
## Conexión RDS

host = 'ds4ateam77.cxdzzcrtbiby.us-east-2.rds.amazonaws.com'
port = 5432
user = 'postgres'
password = '12345678'
database = 'teate'

connDB = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
conn = connDB.raw_connection()
cur = conn.cursor()

def upload_nueva_data():
    nueva_data = pd.read_csv('nueva_data2.csv', dtype='str')
    nueva_data.head(0).to_sql('nueva_data', con=connDB, if_exists='replace', index=False)
    print('Created')

    # Clean special characters
    nueva_data = nueva_data.replace('\n', '', regex=True)
    nueva_data = nueva_data.replace('\r', '', regex=True)

    # Masive load data from python
    output = io.StringIO()
    nueva_data.to_csv(output, sep='|', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'nueva_data', sep='|')
    print('Ready for commit')
    conn.commit()
    print('Commited')
    conn.close()
    print("Upload Finished " + 'nueva_data')

########################################################################################################################
## Monta nueva data a RDS

def runQuery(sql):
    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}',max_overflow=30)
    result = engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute((text(sql)))
    return pd.DataFrame(result.fetchall(), columns=result.keys())

def queries_upload():
    try:
        runQuery("""
            DROP TABLE fact_orders2;
            CREATE TABLE IF NOT EXISTS fact_orders2 as (
             with base as (
                select 
                    index::TEXT, pedido::TEXT, fechapedido::DATE,
                    tienda::TEXT, nombretienda::TEXT,
                   direcciontienda::TEXT, fabricante::TEXT, nombrefabricante::TEXT,
                   material::TEXT, nombrematerial::TEXT, cantidaddepedido::INT,
                   valorunitariopedido::INT,  valortotalpedido::INT,
                   entrega::TEXT, factura::TEXT, zona::TEXT, nombrezonacomercial::TEXT, ruta::TEXT,
                   nombreruta::TEXT, comuna, barrio::TEXT, poblacion::TEXT, 
                    row_number() over(partition by pedido, material  order by valorunitariopedido desc) as row_number,
                    EXTRACT(MONTH FROM fechapedido::DATE)::INT AS month,
                    EXTRACT(YEAR FROM fechapedido::DATE)::INT AS year,
                    EXTRACT(DAY FROM fechapedido::DATE)::INT AS day
                from fact_orders_clean
            ),
            base2 as(
                select * from base
                where row_number =1)
    
            select *
            from base2
            )
            """)
    except:
        print('Ready query 1')
    try:
        runQuery("""
            DROP TABLE nueva_data2;
            CREATE TABLE IF NOT EXISTS nueva_data2 as (
            with base as (
                select 
                    index::TEXT, pedido::TEXT, fechapedido::DATE,
                    tienda::TEXT, nombretienda::TEXT,
                   direcciontienda::TEXT, fabricante::TEXT, nombrefabricante::TEXT,
                   material::TEXT, nombrematerial::TEXT, cantidaddepedido::INT,
                   valorunitariopedido::INT,  valortotalpedido::INT,
                   entrega::TEXT, factura::TEXT, zona::TEXT, nombrezonacomercial::TEXT, ruta::TEXT,
                   nombreruta::TEXT, comuna, barrio::TEXT, poblacion::TEXT, 
                    row_number() over(partition by pedido, material  order by valorunitariopedido desc) as row_number,
                    EXTRACT(MONTH FROM fechapedido::DATE)::INT AS month,
                    EXTRACT(YEAR FROM fechapedido::DATE)::INT AS year,
                    EXTRACT(DAY FROM fechapedido::DATE)::INT AS day
                from nueva_data
            ),
            base2 as(
                select * from base
                where row_number =1)
    
            select *
            from base2
            )
            """)
    except:
        print('Ready query 2')
    try:
        runQuery("""
            DROP TABLE order_facts_clean;
            CREATE TABLE IF NOT EXISTS order_facts_clean as (
                select * from nueva_data2 union all
                select * from fact_orders2)
            """)
    except:
        print('Ready query 3')

def __main__():

    historico, nueva_data, charge = download_history()
    # If there are no new files then exit
    if charge == False:
        quit()

    errors, errors_index_rows, validation = data_validation(nueva_data)
    # If the file has errors print the errors and quit
    if validation == False:
        print(errors, errors_index_rows)
        quit()

    unification(historico, nueva_data)
    upload_nueva_data()
    queries_upload()

finish =True
while finish == True:

    __main__()
    time.sleep(86400)