# Databricks notebook source
import dlt
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, quarter, date_format

# --- Camada REFINED (Ouro) ---
# O objetivo desta camada é modelar os dados para análise (Star Schema).

# --- DIMENSÕES ---

@dlt.table(
  name="d_hoteis",
  comment="Dimensão de Hotéis com informações descritivas."
)
def d_hoteis():
  return spark.read.table("production.trusted.hoteis_trusted")

@dlt.table(
  name="d_hospedes",
  comment="Dimensão de Hóspedes com informações demográficas."
)
def d_hospedes():
  return spark.read.table("production.trusted.hospedes_trusted")

@dlt.table(
  name="d_quartos",
  comment="Dimensão de Quartos com suas características."
)
def d_quartos():
  return spark.read.table("production.trusted.quartos_trusted")

# --- FACT TABLES ---

@dlt.table(
  name="f_reservas",
  comment="Tabela de factos central com as métricas de reservas."
)
def f_reservas():
  # Lê a tabela principal de factos
  reservas = spark.read.table("production.trusted.reservas_trusted")
  # Lê a tabela de canais para juntar a informação do canal
  canais = spark.read.table("production.trusted.reservas_canal_trusted")

  # Junta as duas tabelas para enriquecer os factos
  f_reservas_joined = reservas.join(
    canais,
    reservas.reserva_id == canais.reserva_id,
    "left"
  )

  # Seleciona as chaves estrangeiras e as métricas
  f_reservas_final = f_reservas_joined.select(
    col("reserva_id"),
    col("hospede_id"),
    col("quarto_id"),
    col("hotel_id"),
    col("data_reserva"),
    col("data_checkin"),
    col("data_checkout"),
    col("canal_reserva"),
    col("numero_noites"),
    col("valor_total_estadia")
  )

  return f_reservas_final