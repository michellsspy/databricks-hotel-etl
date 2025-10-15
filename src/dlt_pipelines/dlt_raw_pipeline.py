{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3881723f",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "# Databricks notebook source  # <-- A LINHA MÁGICA QUE FALTAVA\n",
    "import dlt\n",
    "from pyspark.sql.functions import *\n",
    "\n",
    "# --- Camada RAW ---\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"hoteis_raw\",\n",
    "  comment=\"Cópia inicial da tabela de hoteis.\"\n",
    ")\n",
    "def hoteis_raw():\n",
    "  return spark.read.table(\"production.transient.source_hoteis\")\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"quartos_raw\",\n",
    "  comment=\"Cópia inicial da tabela de quartos.\"\n",
    ")\n",
    "def quartos_raw():\n",
    "  return spark.read.table(\"production.transient.source_quartos\")\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"hospedes_raw\",\n",
    "  comment=\"Cópia inicial da tabela de hospedes.\"\n",
    ")\n",
    "def hospedes_raw():\n",
    "  return spark.read.table(\"production.transient.source_hospedes\")\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"consumos_raw\",\n",
    "  comment=\"Cópia inicial da tabela de consumos.\"\n",
    ")\n",
    "def consumos_raw():\n",
    "  return spark.read.table(\"production.transient.source_consumos\")\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"faturas_raw\",\n",
    "  comment=\"Cópia inicial da tabela de faturas.\"\n",
    ")\n",
    "def faturas_raw():\n",
    "  return spark.read.table(\"production.transient.source_faturas\")\n",
    "\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"reservas_raw\",\n",
    "  comment=\"Cópia inicial da tabela de reservas.\"\n",
    ")\n",
    "def reservas_raw():\n",
    "  return spark.read.table(\"production.transient.source_reservas\")\n",
    "\n",
    "\n",
    "@dlt.table(\n",
    "  name=\"reservas_canal_raw\",\n",
    "  comment=\"Cópia inicial da tabela de reservas_canal.\"\n",
    ")\n",
    "def reservas_canal_raw():\n",
    "  return spark.read.table(\"production.transient.source_reservas_canal\")"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
