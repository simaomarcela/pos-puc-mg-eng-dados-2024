# Databricks notebook source
from pyspark.sql.functions import concat_ws,md5,trim,lower,current_timestamp,from_utc_timestamp,lit,col
import os

# COMMAND ----------

dbutils.widgets.text("origem", "")
dbutils.widgets.text("assunto", "")
dbutils.widgets.text("camada", "")
dbutils.widgets.dropdown("mode", "full", ["full", "delta"])
dbutils.widgets.text("sql_query", "")
dbutils.widgets.text("sql_ajust", "")

# dbutils.widgets.removeAll()

# COMMAND ----------

tb_parameters_name = 'datalake.administracao.tb_datalake_parametros'

# COMMAND ----------

origem = dbutils.widgets.get("origem")
assunto = dbutils.widgets.get("assunto")
camada = dbutils.widgets.get("camada")
modo = dbutils.widgets.get("mode")
try:
    sql_ajust = dbutils.widgets.get("sql_ajust")
except:
    sql_ajust=''
try:
    sql_query = dbutils.widgets.get("sql_query")
except:
    sql_query=''

print("origem:",origem)
print("assunto:",assunto)
print("camada:",camada)
print("modo:",modo)
print("sql_query:",sql_query)
print("sql_ajust:",sql_ajust)

# COMMAND ----------


tabela = f'tb_{origem}_{assunto}'

key_id = f'{camada}.{tabela}'

if camada =='bronze':
    pre_camada = 'bronze'
if camada =='silver':
    pre_camada = 'bronze'
if camada =='gold':
    pre_camada = 'silver'

inputPath = f"abfss://{pre_camada}@stpospucmgdatalake.dfs.core.windows.net/{origem}/{assunto}/"

if camada =='bronze':
    outputPath = f"abfss://{camada}@stpospucmgdatalake.dfs.core.windows.net/{origem}/{assunto}_delta"
else:
    outputPath = f"abfss://{camada}@stpospucmgdatalake.dfs.core.windows.net/{origem}/{assunto}/"
    

if sql_query =='':
    sql_query=f'select a.* ,  from_utc_timestamp(current_timestamp(), "America/Sao_Paulo")  as dat_incl_datalake_{camada} from {pre_camada}.{tabela} a'


print("inputPath:", inputPath)
print("outputPath:", outputPath)
print("tabela:", tabela)
print("key_id:", key_id)
print("pre_camada:", pre_camada)
print("sql_query:", sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists administracao.tb_datalake_parametros (
# MAGIC   key_id STRING,
# MAGIC   origem STRING,
# MAGIC   assunto STRING,
# MAGIC   camada STRING,
# MAGIC   modo STRING,
# MAGIC   path STRING,
# MAGIC   sql_query STRING,
# MAGIC   sql_ajust STRING,
# MAGIC   hash_coluna STRING,
# MAGIC   data_incl_datalake timestamp,
# MAGIC   data_upd_datalake timestamp
# MAGIC )

# COMMAND ----------

#Monta a query para recuperar as informacoes na tabela de parametro
query =f"select key_id,  origem, assunto,  camada,modo ,path ,sql_query , sql_ajust, hash_coluna from {tb_parameters_name} where (key_id ='{key_id}' and modo = '{modo}')"

query

# COMMAND ----------

#Cria o dataframe baseado na recuperacao da tabela de parametros
df_param=spark.sql(query)
display(df_param)

# COMMAND ----------

# retorna a quantidade de linhas do dataframe df_param
qtde_linhas = df_param.count()
qtde_linhas

# COMMAND ----------

schema = ['key_id', 'origem', 'assunto', 'camada','modo', 'path','sql_query','sql_ajust']

# COMMAND ----------


#Cria lista com os parametros recebidos
ls_parametros =[]
ls_parametros.append([key_id,  origem, assunto,  camada,modo ,inputPath ,sql_query , sql_ajust])
df_parametros = spark.createDataFrame(ls_parametros, schema)

#Inclui coluna hash para comparacao posterior de atualizacao
df_parametros = df_parametros.withColumn("hash_coluna", md5(concat_ws("", trim(lower(df_parametros.sql_query)),df_parametros.sql_ajust,df_parametros.path)))

#Inclui coluna de data inclusao
df_parametros = df_parametros.withColumn("data_incl_datalake", from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))

#Inclui coluna de data atualizacao
df_parametros = df_parametros.withColumn("data_upd_datalake", from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))

df_parametros.display()

# COMMAND ----------

def insert_tb_parameters(df, tb_parameters_name):
    '''
    Insere ou atualiza linhas na tabela tb_parameters baseado nas colunas do dataframe df_param
    
    Parametros:
    df (Dataframe): Dataframe com as colunas a serem inseridas/atualizadas na tabela tb_parameters
    tb_parameters_name (str): Nome da tabela tb_parameters no catalogo do Spark
    
    Retorna:
    str: Mensagem informando se houve sucesso ou falha na operacao
    '''
    # retorna a quantidade de linhas do dataframe df
    qtde = df.count()
    
    # Atualiza tabela caso ela ja tenha registros
    if qtde > 0:
        #nova insercao
        if qtde_linhas ==0:
            df.write.mode('append').format('delta').saveAsTable(tb_parameters_name)
            return "Registro inserido com sucesso."
        if qtde_linhas >0:
            if df.select("hash_coluna").collect()[0][0] != df_param.select("hash_coluna").collect()[0][0]:
                hash_colunas = (df.select("hash_coluna").collect()[0][0])
                spark.sql(f"update {tb_parameters_name}  set sql_query='{sql_query}',  sql_ajust='{sql_ajust}', path = '{inputPath}', data_upd_datalake =from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'), hash_coluna = '{hash_colunas}'  where key_id ='{key_id}' and modo= '{modo}' ")
                return "Registro atualizado com sucesso."
            else:
                return "Nao ha informacao para atualizar a tabela, hash dos registros são iguais"
    else:
        return "Nao ha registros para atualizar a tabela."


# COMMAND ----------

insert_tb_parameters(df=df_parametros,tb_parameters_name=tb_parameters_name)

# COMMAND ----------

#leitura
if camada == 'bronze':
    data = spark.read.parquet(inputPath)
    data=data.withColumn("data_incl_datalake_bronze", from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))
else:
    data = spark.sql(sql_query)

# COMMAND ----------

if modo =='full':
    mode = 'overwrite'
if modo == 'delta': 
    mode = 'delta'

data.write.mode(mode).option("overwriteSchema", "true").format("delta").save(outputPath)

spark.sql(f"CREATE TABLE IF NOT EXISTS {key_id} USING DELTA LOCATION '{outputPath}'")
