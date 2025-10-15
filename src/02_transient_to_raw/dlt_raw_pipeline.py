# Databricks notebook source  # <-- A LINHA MÁGICA QUE FALTAVA
import dlt
from pyspark.sql.functions import *

# --- Camada RAW ---

@dlt.table(
  name="hoteis_raw",
  comment="Cópia inicial da tabela de hoteis."
)
def hoteis_raw():
  return spark.read.table("production.transient.source_hoteis")

@dlt.table(
  name="quartos_raw",
  comment="Cópia inicial da tabela de quartos."
)
def quartos_raw():
  return spark.read.table("production.transient.source_quartos")

@dlt.table(
  name="hospedes_raw",
  comment="Cópia inicial da tabela de hospedes."
)
def hospedes_raw():
  return spark.read.table("production.transient.source_hospedes")

@dlt.table(
  name="consumos_raw",
  comment="Cópia inicial da tabela de consumos."
)
def consumos_raw():
  return spark.read.table("production.transient.source_consumos")

@dlt.table(
  name="faturas_raw",
  comment="Cópia inicial da tabela de faturas."
)
def faturas_raw():
  return spark.read.table("production.transient.source_faturas")


@dlt.table(
  name="reservas_raw",
  comment="Cópia inicial da tabela de reservas."
)
def reservas_raw():
  return spark.read.table("production.transient.source_reservas")


@dlt.table(
  name="reservas_canal_raw",
  comment="Cópia inicial da tabela de reservas_canal."
)
def reservas_canal_raw():
  return spark.read.table("production.transient.source_reservas_canal")