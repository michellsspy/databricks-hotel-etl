# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

# --- Camada TRUSTED (Prata) ---
# O objetivo desta camada é limpar, padronizar e enriquecer os dados.

@dlt.table(
  name="hoteis_trusted",
  comment="Tabela de hoteis limpa e padronizada."
)
def hoteis_trusted():
  df_hoteis_raw = spark.read.table("production.raw.hoteis_raw")
  
  df_trusted = df_hoteis_raw.select(
    col("hotel_id").cast("INT"),
    col("nome_hotel").cast("STRING"),
    col("endereco").cast("STRING"),
    col("cidade").cast("STRING"),
    col("estado").cast("STRING"),
    col("estrelas").cast("INT"),
    col("numero_quartos").cast("INT"),
    col("comodidades").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())
  
  return df_trusted


@dlt.table(
  name="quartos_trusted",
  comment="Tabela de quartos limpa e com tipos de dados garantidos."
)
def quartos_trusted():
  df_quartos_raw = spark.read.table("production.raw.quartos_raw")

  df_trusted = df_quartos_raw.select(
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("numero_quarto").cast("STRING"),
      col("tipo_quarto").cast("STRING"),
      col("capacidade_maxima").cast("INT"),
      col("preco_diaria_base").cast("DOUBLE")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="hospedes_trusted",
  comment="Tabela de hóspedes limpa e padronizada."
)
def hospedes_trusted():
  df_hospedes_raw = spark.read.table("production.raw.hospedes_raw")

  df_trusted = df_hospedes_raw.select(
      col("hospede_id").cast("INT"),
      col("nome_completo").cast("STRING"),
      col("cpf").cast("STRING"),
      col("data_nascimento").cast("DATE"),
      col("email").cast("STRING"),
      col("estado").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="reservas_trusted",
  comment="Tabela de reservas com dados limpos e tipados."
)
def reservas_trusted():
  df_reservas_raw = spark.read.table("production.raw.reservas_raw")

  df_trusted = df_reservas_raw.select(
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("data_reserva").cast("DATE"),
      col("data_checkin").cast("DATE"),
      col("data_checkout").cast("DATE"),
      col("numero_noites").cast("INT"),
      col("valor_total_estadia").cast("DOUBLE")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="reservas_canal_trusted",
  comment="Tabela de canais de reserva limpa."
)
def reservas_canal_trusted():
  df_reservas_canal_raw = spark.read.table("production.raw.reservas_canal_raw")

  df_trusted = df_reservas_canal_raw.select(
      col("reserva_canal_id").cast("STRING"),
      col("reserva_id").cast("INT"),
      col("canal_reserva").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="consumos_trusted",
  comment="Tabela de consumos com valores e datas padronizados."
)
def consumos_trusted():
  df_consumos_raw = spark.read.table("production.raw.consumos_raw")

  df_trusted = df_consumos_raw.select(
      col("consumo_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("data_consumo").cast("DATE"),
      col("tipo_servico").cast("STRING"),
      col("valor").cast("DOUBLE")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="faturas_trusted",
  comment="Tabela de faturas com valores e datas padronizados."
)
def faturas_trusted():
  df_faturas_raw = spark.read.table("production.raw.faturas_raw")

  df_trusted = df_faturas_raw.select(
      col("fatura_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("data_emissao").cast("DATE"),
      col("valor_total").cast("DOUBLE"),
      col("forma_pagamento").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted