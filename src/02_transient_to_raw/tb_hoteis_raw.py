# Databricks notebook source
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("RawHoteis").getOrCreate()

print("Lendo a tabela da camada transient...")
# Lê a tabela de origem que foi gerada pelo primeiro script
df_source = spark.table("production.transient.source_hoteis")

print("Gravando na camada raw...")
# Escreve os dados na camada raw sem transformações
df_source.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("production.raw.hoteis")

print("Tabela 'hoteis' movida para a camada Raw com sucesso.")