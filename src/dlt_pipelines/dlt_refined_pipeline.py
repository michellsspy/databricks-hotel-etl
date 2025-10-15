# Databricks notebook source
import dlt
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, quarter, date_format

# --- Camada REFINED (Ouro) ---

# --- DIMENSÕES ---

@dlt.table(name="d_hoteis", comment="Dimensão de Hotéis com informações descritivas.")
def d_hoteis():
  return spark.read.table("production.trusted.hoteis_trusted")

@dlt.table(name="d_hospedes", comment="Dimensão de Hóspedes com informações demográficas.")
def d_hospedes():
  return spark.read.table("production.trusted.hospedes_trusted")

@dlt.table(name="d_quartos", comment="Dimensão de Quartos com suas características.")
def d_quartos():
  return spark.read.table("production.trusted.quartos_trusted")

# --- FACT TABLES ---

@dlt.table(name="f_reservas", comment="Tabela de factos central com as métricas de reservas.")
def f_reservas():
  reservas = spark.read.table("production.trusted.reservas_trusted")
  canais = spark.read.table("production.trusted.reservas_canal_trusted")

  f_reservas_joined = reservas.join(
    canais,
    # A condição do join continua a mesma
    reservas.reserva_id == canais.reserva_id,
    "left"
  )

  f_reservas_final = f_reservas_joined.select(
    # MUDANÇA CRUCIAL: Especificamos de qual tabela vem o 'reserva_id'
    reservas.reserva_id, 
    reservas.hospede_id,
    reservas.quarto_id,
    reservas.hotel_id,
    reservas.data_reserva,
    reservas.data_checkin,
    reservas.data_checkout,
    canais.canal_reserva,
    reservas.numero_noites,
    reservas.valor_total_estadia
  )
  
  return f_reservas_final