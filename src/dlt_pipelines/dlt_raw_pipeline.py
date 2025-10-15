# Databricks notebook source
import dlt

# --- Camada RAW (Ingestão Pura) ---
# Esta camada agora apenas anexa (appends) os novos dados recebidos da fonte,
# criando um log imutável e completo. Este é um processo em batch incremental.

@dlt.table(name="hoteis_raw", comment="Carga Full da tabela de hoteis.")
def hoteis_raw():
  return spark.read.table("production.transient.source_hoteis")

@dlt.table(name="quartos_raw", comment="Carga Full da tabela de quartos.")
def quartos_raw():
  return spark.read.table("production.transient.source_quartos")

# As tabelas abaixo são incrementais (apenas append)
@dlt.table(name="hospedes_raw", comment="Log incremental de alterações de hóspedes.")
def hospedes_raw():
  return spark.readStream.table("production.transient.source_hospedes")

@dlt.table(name="reservas_raw", comment="Log incremental de reservas.")
def reservas_raw():
  return spark.readStream.table("production.transient.source_reservas")

@dlt.table(name="reservas_canal_raw", comment="Log incremental de canais de reserva.")
def reservas_canal_raw():
  return spark.readStream.table("production.transient.source_reservas_canal")

@dlt.table(name="consumos_raw", comment="Log incremental de consumos.")
def consumos_raw():
  return spark.readStream.table("production.transient.source_consumos")

@dlt.table(name="faturas_raw", comment="Log incremental de faturas.")
def faturas_raw():
  return spark.readStream.table("production.transient.source_faturas")