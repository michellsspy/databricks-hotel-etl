# Databricks notebook source
import dlt
from pyspark.sql.functions import col, monotonically_increasing_id, to_date, year, month, dayofmonth, quarter, date_format, expr

# --- Camada REFINED (Ouro) ---
# O objetivo desta camada é modelar os dados para análise (Star Schema).

# --- DIMENSÕES EXISTENTES (LIMPAS) ---
# Estas dimensões são cópias diretas da camada Trusted, pois já são puramente descritivas.

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


# --- NOVAS DIMENSÕES CRIADAS A PARTIR DOS DADOS ---

@dlt.table(
  name="d_canal",
  comment="Dimensão de Canais de Reserva."
)
def d_canal():
  # Extrai os canais únicos da tabela de canais da camada trusted
  return (
    spark.read.table("production.trusted.reservas_canal_trusted")
      .select("canal_reserva")
      .distinct()
      .withColumn("id_canal", monotonically_increasing_id()) # Cria uma chave substituta (surrogate key)
  )

@dlt.table(
  name="d_tempo",
  comment="Dimensão de Tempo com atributos de calendário."
)
def d_tempo():
  # Cria a dimensão de tempo a partir de todas as datas únicas presentes nas reservas
  datas_reservas = spark.read.table("production.trusted.reservas_trusted").select(col("data_reserva").alias("data"))
  datas_checkin = spark.read.table("production.trusted.reservas_trusted").select(col("data_checkin").alias("data"))
  datas_checkout = spark.read.table("production.trusted.reservas_trusted").select(col("data_checkout").alias("data"))

  # Junta todas as datas, remove duplicados e nulos
  datas_unicas = datas_reservas.union(datas_checkin).union(datas_checkout).distinct().filter(col("data").isNotNull())

  # Enriquece cada data com atributos de calendário
  return (
    datas_unicas
      .withColumn("ano", year(col("data")))
      .withColumn("mes", month(col("data")))
      .withColumn("dia", dayofmonth(col("data")))
      .withColumn("trimestre", quarter(col("data")))
      .withColumn("nome_dia_semana", date_format(col("data"), "EEEE"))
      .withColumn("nome_mes", date_format(col("data"), "MMMM"))
      .select(
        col("data").alias("id_data"), # A própria data serve como chave
        "ano", "mes", "dia", "trimestre", "nome_dia_semana", "nome_mes"
      )
  )


# --- RECONSTRUÇÃO DA TABELA DE FACTOS ---

@dlt.table(
  name="f_reservas",
  comment="Tabela de factos central, ligada a todas as dimensões."
)
def f_reservas():
  # Lê as tabelas da camada trusted
  reservas = spark.read.table("production.trusted.reservas_trusted")
  canais = spark.read.table("production.trusted.reservas_canal_trusted")

  # Lê as novas dimensões que acabámos de criar neste pipeline
  d_canal_stream = dlt.read("d_canal")
  d_tempo_stream = dlt.read("d_tempo")

  # Passo 1: Junta reservas com canais para obter o nome do canal
  reservas_com_canal = reservas.join(canais, ["reserva_id"], "left")

  # Passo 2: Junta com a dimensão d_canal para obter a chave substituta id_canal
  reservas_com_id_canal = reservas_com_canal.join(d_canal_stream, ["canal_reserva"], "left")
  
  # Passo 3: Junta TRÊS VEZES com a dimensão de tempo para obter as chaves de data
  reservas_finais = (
    reservas_com_id_canal
      .join(d_tempo_stream.alias("tempo_reserva"), col("data_reserva") == col("tempo_reserva.id_data"), "left")
      .join(d_tempo_stream.alias("tempo_checkin"), col("data_checkin") == col("tempo_checkin.id_data"), "left")
      .join(d_tempo_stream.alias("tempo_checkout"), col("data_checkout") == col("tempo_checkout.id_data"), "left")
  )

  # Passo 4: Seleciona apenas as chaves e as métricas para a tabela de factos final
  return reservas_finais.select(
    # Chaves Estrangeiras
    col("reserva_id"), # Mantemos como chave de negócio degenerada
    col("hospede_id"),
    col("quarto_id"),
    col("hotel_id"),
    col("id_canal"),
    col("tempo_reserva.id_data").alias("id_data_reserva"),
    col("tempo_checkin.id_data").alias("id_data_checkin"),
    col("tempo_checkout.id_data").alias("id_data_checkout"),
    # Métricas
    col("numero_noites"),
    col("valor_total_estadia")
  )