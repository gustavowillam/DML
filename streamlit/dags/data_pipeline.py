#airflow no windows não funciona 
#para funcionar os imports do airflow foram alterados os arquivos 

# C:\ProgramData\Anaconda3\Lib\pty.py na linha 12

#if os.name != "nt":  #tty no Windows faz import em termios -> termios não funciona no windows 
#   import tty       


#C:\ProgramData\Anaconda3\Lib\site-packages\airflow\utils\process_utils.py na linha 30

#if os.name != "nt":  #termios não funciona no windows e tty no Windows faz import em termios 
#   import termios
#   import tty


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sqlite3
import pandas as pd

default_args = {'owner': 'airflow'}

#path = "C://Users//gusta//Meu Drive//Colab Notebooks//DML//DataSets"
path = "/home/gustavo/Documentos/api_streamlit/data"  #Linux

path_db_producao = path + "/imoveis_prod.db"

path_db_datawharehouse = path + "/imoveis_dw.db"

path_temp_csv = path + "/imoveis.csv"


dag = DAG(dag_id='data_pipeline',  default_args=default_args, schedule_interval='@daily', start_date=days_ago(2),)


def _extract():
    #conectando a base de dados de produção.
    connect_db_imoveis = sqlite3.connect(path_db_producao)
    
    #selecionando os dados.
    dataset_df = pd.read_sql_query(r"""
        SELECT CIDADE.NOME as 'cidade'
       ,ESTADO.NOME as 'estado'
       ,IMOVEIS.AREA as 'area'
       ,IMOVEIS.NUM_QUARTOS
       ,IMOVEIS.NUM_BANHEIROS
       ,IMOVEIS.GARAGEM       
       ,IMOVEIS.NUM_ANDARES
       ,IMOVEIS.ACEITA_ANIMAIS
       ,IMOVEIS.MOBILIA
       ,IMOVEIS.VALOR_ALUGUEL
       ,IMOVEIS.VALOR_CONDOMINIO
       ,IMOVEIS.VALOR_IPTU
       ,IMOVEIS.VALOR_SEGURO_INCENDIO

        FROM IMOVEIS INNER JOIN CIDADE
        ON IMOVEIS.CODIGO_CIDADE = CIDADE.CODIGO
        INNER JOIN ESTADO
        ON CIDADE.CODIGO_ESTADO = ESTADO.CODIGO;
        """, 
        connect_db_imoveis
        )
    
    #exportando os dados para a area de stage (arquivo .csv)
    dataset_df.to_csv(path_temp_csv,   index=False)

    #fechando a conexão com o banco de dados.
    connect_db_imoveis.close()


def _transform():
    
    dataset_df = pd.read_csv(path_temp_csv)

    #transformando os dados dos atributos.
    dataset_df.aceita_animais.replace({'acept': 1, 'not acept':0}, inplace=True)
    dataset_df.mobilia.replace({'furnished': 1, 'not furnished':0}, inplace=True)

    #limpando os registros.
    dataset_df.num_andares.replace({'-': 1}, inplace=True)
    dataset_df.cidade = dataset_df.cidade.str.title()
    dataset_df.cidade.replace({'São Paulo': 'Sao Paulo', 'Rio De Janeiro': 'Rio de Janeiro'},  inplace=True)

    dataset_df.to_csv(path+"//imoveis.csv",  index=False)


def _load():
    #conectando com o banco de dados Data Wharehouse.
    connect_db_imoveis_dw = sqlite3.connect(path_db_datawharehouse)
    
    #lendo os dados a partir dos arquivos csv.
    dataset_df = pd.read_csv(path_temp_csv)

    #carregando os dados no banco de dados no arquivo _dw.db.
    dataset_df.to_sql("imoveis", connect_db_imoveis_dw, if_exists="replace",index=False)



#nao vao funcionar estes comandos por causa do airflow que nao funciona no windows

extract_task = PythonOperator(task_id="extract", python_callable=_extract, dag=dag)

transform_task = PythonOperator(task_id="transform", python_callable=_transform, dag=dag)

load_task = PythonOperator(task_id="load", python_callable=_load, dag=dag)

#ETL 
extract_task >> transform_task >> load_task


#pd.__version__
#import fsspec 
#fsspec.__version__

#import sklearn
#sklearn.__version__

#import  scipy 
#cipy.__version__


#executando direto as fun��es para fazer a ETL do BD Sqlite para um arquivo csv 
#_extract()
#_transform()
#_load()




