# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

# Lemos da nossa tabela da camada Raw.
# O DLT entende que esta tabela depende da 'hoteis_raw' do outro notebook.
@dlt.table(
  name="hoteis_trusted",
  comment="Tabela de hoteis limpa e padronizada."
)
def hoteis_trusted():
  # dlt.read() é a forma de ler uma tabela que faz parte do mesmo pipeline DLT
  df_hoteis_raw = dlt.read("hoteis_raw")
  
  # Aplicamos as transformações
  df_trusted = df_hoteis_raw.select(
    col("hotel_id"),
    col("nome_hotel"),
    col("endereco"),
    col("cidade"),
    col("estado"),
    col("estrelas"),
    col("numero_quartos"),
    col("comodidades")
  ).withColumn("data_carga_trusted", current_timestamp()) # Adicionamos uma coluna de auditoria
  
  return df_trusted