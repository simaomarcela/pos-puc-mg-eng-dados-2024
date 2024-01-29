# Databricks notebook source
#Informacoes da pipeline
data_factory_name = dbutils.widgets.get("data_factory_name")
pipeline_name = dbutils.widgets.get("pipeline_name")

#Informacoes assunto pipeline
pip_modo = dbutils.widgets.get("pip_modo")
origem = dbutils.widgets.get("origem")
assunto = dbutils.widgets.get("assunto")
tabela = dbutils.widgets.get("tabela")

#Informacoes trigger
pip_trigger_id = dbutils.widgets.get("pip_trigger_id")
pip_trigger_name = dbutils.widgets.get("pip_trigger_name")
pip_trigger_time = dbutils.widgets.get("pip_trigger_time")
pip_trigger_type = dbutils.widgets.get("pip_grp_id")

#Informacoes Execucao
pip_grp_id = dbutils.widgets.get("pip_grp_id")
pip_run_id = dbutils.widgets.get("pip_run_id")


#Informacoes Copy Data
pip_cpy_source = dbutils.widgets.get("pip_cpy_source")
pip_cpy_duration = dbutils.widgets.get("pip_cpy_duration")
pip_cpy_exc_dius = dbutils.widgets.get("pip_cpy_exc_dius")
pip_cpy_exc_start = dbutils.widgets.get("pip_cpy_exc_start")
pip_cpy_exc_status = dbutils.widgets.get("pip_cpy_exc_status")
pip_cpy_row_copied = dbutils.widgets.get("pip_cpy_row_copied")
pip_cpy_row_read = dbutils.widgets.get("pip_cpy_row_read")


#Informacoes Camadas
pip_nt_exe_bronze = dbutils.widgets.get("pip_nt_exe_bronze")
pip_nt_exe_silver = dbutils.widgets.get("pip_nt_exe_silver")
pip_nt_exe_gold = dbutils.widgets.get("pip_nt_exe_gold")


# COMMAND ----------

def update_tb_adf_execucoes(**kwargs):
    #O **kwargs é uma palavra-chave do Python que permite passar um número variável de argumentos para uma função.
    #Ele permite que você passe vários argumentos-chave para uma função em vez de apenas um.
  from pyspark.sql.functions import when
  from pyspark.sql.types import StringType
  
  catalog_database_name = "administracao"
  catalog_table_name = "tb_adf_execucoes"
  
  pip_run_id = kwargs.get("pip_run_id")
  
  # Checar se a tabela existe
  try:
    if (spark.sql(f"select * from {catalog_database_name}.{catalog_table_name}")).collect() > 0:
      # Checar se a execução já existe na tabela
      if len(spark.sql(f"SELECT * FROM {catalog_database_name}.{catalog_table_name} WHERE pip_run_id='{pip_run_id}'").collect()) > 0:
        # Atualiza a linha existente
        update_query = f"""
          UPDATE {catalog_database_name}.{catalog_table_name} SET 
          data_factory_name='{kwargs.get('data_factory_name')}',
          pipeline_name='{kwargs.get('pipeline_name')}',
          pip_trigger_id='{kwargs.get('pip_trigger_id')}',
          pip_trigger_name='{kwargs.get('pip_trigger_name')}',
          pip_trigger_time='{kwargs.get('pip_trigger_time')}',
          pip_trigger_type='{kwargs.get('pip_trigger_type')}',
          pip_grp_id='{kwargs.get('pip_grp_id')}',
          pip_cpy_source='{kwargs.get('pip_cpy_source')}',
          pip_cpy_duration='{kwargs.get('pip_cpy_duration')}',
          pip_cpy_exc_dius='{kwargs.get('pip_cpy_exc_dius')}',
          pip_cpy_exc_start='{kwargs.get('pip_cpy_exc_start')}',
          pip_cpy_exc_status='{kwargs.get('pip_cpy_exc_status')}',
          pip_cpy_row_copied='{kwargs.get('pip_cpy_row_copied')}',
          pip_cpy_row_read='{kwargs.get('pip_cpy_row_read')}',
          pip_modo='{kwargs.get('pip_modo')}',
          origem='{kwargs.get('origem')}',
          assunto='{kwargs.get('assunto')}',
          tabela='{kwargs.get('tabela')}',
          pip_nt_exe_bronze='{kwargs.get('pip_nt_exe_bronze')}',
          pip_nt_exe_silver='{kwargs.get('pip_nt_exe_silver')}',
          pip_nt_exe_gold='{kwargs.get('pip_nt_exe_gold')}'
          WHERE pip_run_id='{pip_run_id}'
        """
        spark.sql(update_query)
      else:
        print('passei aqui 4')
        # Insere nova linha
        insert_query = f"""
          INSERT INTO {catalog_database_name}.{catalog_table_name} (
            data_factory_name,
            pipeline_name,
            pip_trigger_id,
            pip_trigger_name,
            pip_trigger_time,
            pip_trigger_type,
            pip_grp_id,
            pip_run_id,
            pip_cpy_source,
            pip_cpy_duration,
            pip_cpy_exc_dius,
            pip_cpy_exc_start,
            pip_cpy_exc_status,
            pip_cpy_row_copied,
            pip_cpy_row_read,
            pip_modo,
            origem,
            assunto,
            tabela,
            pip_nt_exe_bronze,
            pip_nt_exe_silver,
            pip_nt_exe_gold
            WHERE pip_run_id='{pip_run_id}'
          )
          VALUES (
            '{kwargs.get('data_factory_name')}',
            '{kwargs.get('pipeline_name')}',
            '{kwargs.get('pip_trigger_id')}',
            '{kwargs.get('pip_trigger_name')}',
            '{kwargs.get('pip_trigger_time')}',
            '{kwargs.get('pip_trigger_type')}',
            '{kwargs.get('pip_grp_id')}',
            '{pip_run_id}',
            '{kwargs.get('pip_cpy_source')}',
            '{kwargs.get('pip_cpy_duration')}',
            '{kwargs.get('pip_cpy_exc_dius')}',
            '{kwargs.get('pip_cpy_exc_start')}',
            '{kwargs.get('pip_cpy_exc_status')}',
            '{kwargs.get('pip_cpy_row_copied')}',
            '{kwargs.get('pip_cpy_row_read')}',
            '{kwargs.get('pip_modo')}',
            '{kwargs.get('origem')}',
            '{kwargs.get('assunto')}',
            '{kwargs.get('tabela')}',
            '{kwargs.get('pip_nt_exe_bronze')}',
            '{kwargs.get('pip_nt_exe_silver')}',
            '{kwargs.get('pip_nt_exe_gold')}'
            WHERE pip_run_id='{pip_run_id}'
          )
        """
        spark.sql(insert_query)
  except:
    print('passei aqui 5')
    # Cria a tabela
    create_query = f"""
      CREATE TABLE if not exists {catalog_database_name}.{catalog_table_name} (
        data_factory_name STRING,
        pipeline_name STRING,
        pip_trigger_id STRING,
        pip_trigger_name STRING,
        pip_trigger_time STRING,
        pip_trigger_type STRING,
        pip_grp_id STRING,
        pip_run_id STRING,
        pip_cpy_source STRING,
        pip_cpy_duration STRING,
        pip_cpy_exc_dius STRING,
        pip_cpy_exc_start STRING,
        pip_cpy_exc_status STRING,
        pip_cpy_row_copied STRING,
        pip_cpy_row_read STRING,
        pip_modo STRING,
        origem STRING,
        assunto STRING,
        tabela STRING,
        pip_nt_exe_bronze STRING,
        pip_nt_exe_silver STRING,
        pip_nt_exe_gold STRING
      )
    """
    spark.sql(create_query)
    
    # Insere nova linha
    insert_query = f"""
      INSERT INTO {catalog_database_name}.{catalog_table_name} (
        data_factory_name,
        pipeline_name,
        pip_trigger_id,
        pip_trigger_name,
        pip_trigger_time,
        pip_trigger_type,
        pip_grp_id,
        pip_run_id,
        pip_cpy_source,
        pip_cpy_duration,
        pip_cpy_exc_dius,
        pip_cpy_exc_start,
        pip_cpy_exc_status,
        pip_cpy_row_copied,
        pip_cpy_row_read,
        pip_modo,
        origem,
        assunto,
        tabela,
        pip_nt_exe_bronze,
        pip_nt_exe_silver,
        pip_nt_exe_gold
    
      )
      VALUES (
        '{kwargs.get('data_factory_name')}',
        '{kwargs.get('pipeline_name')}',
        '{kwargs.get('pip_trigger_id')}',
        '{kwargs.get('pip_trigger_name')}',
        '{kwargs.get('pip_trigger_time')}',
        '{kwargs.get('pip_trigger_type')}',
        '{kwargs.get('pip_grp_id')}',
        '{pip_run_id}',
        '{kwargs.get('pip_cpy_source')}',
        '{kwargs.get('pip_cpy_duration')}',
        '{kwargs.get('pip_cpy_exc_dius')}',
        '{kwargs.get('pip_cpy_exc_start')}',
        '{kwargs.get('pip_cpy_exc_status')}',
        '{kwargs.get('pip_cpy_row_copied')}',
        '{kwargs.get('pip_cpy_row_read')}',
        '{kwargs.get('pip_modo')}',
        '{kwargs.get('origem')}',
        '{kwargs.get('assunto')}',
        '{kwargs.get('tabela')}',
        '{kwargs.get('pip_nt_exe_bronze')}',
        '{kwargs.get('pip_nt_exe_silver')}',
        '{kwargs.get('pip_nt_exe_gold')}'
      )
    """
    spark.sql(insert_query)


# COMMAND ----------

update_tb_adf_execucoes(
    data_factory_name = data_factory_name,
    pipeline_name = pipeline_name,
    pip_trigger_id = pip_trigger_id,
    pip_trigger_name = pip_trigger_name,
    pip_trigger_time = pip_trigger_time,
    pip_trigger_type = pip_trigger_type,
    pip_grp_id = pip_grp_id,
    pip_run_id = pip_run_id,
    pip_cpy_source = pip_cpy_source,
    pip_cpy_duration = pip_cpy_duration,
    pip_cpy_exc_dius = pip_cpy_exc_dius,
    pip_cpy_exc_start = pip_cpy_exc_start,
    pip_cpy_exc_status = pip_cpy_exc_status,
    pip_cpy_row_copied = pip_cpy_row_copied,
    pip_cpy_row_read = pip_cpy_row_read,
    pip_modo =pip_modo,
    origem = origem,
    assunto =assunto,
    tabela = tabela,
    pip_nt_exe_bronze = pip_nt_exe_bronze,
    pip_nt_exe_silver = pip_nt_exe_silver,
    pip_nt_exe_gold = pip_nt_exe_gold
)
