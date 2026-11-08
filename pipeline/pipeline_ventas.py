# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline declarativo de ventas
# MAGIC
# MAGIC Este archivo se agrega como código fuente de un pipeline Lakeflow Spark
# MAGIC Declarative Pipelines con publicación en el catálogo y esquema definidos.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "ingenieria_datos_jjrp"
TABLE_BRONZE = f"{CATALOG}.{SCHEMA}.ventas_bronze"

# COMMAND ----------

@dp.materialized_view(
    name="pipeline_ventas_limpias",
    comment="Ventas válidas sin duplicados con total_venta calculado.",
)
def pipeline_ventas_limpias():
    return (
        spark.read.table(TABLE_BRONZE)
        .dropDuplicates()
        .filter(F.col("producto").isNotNull())
        .filter(F.length(F.trim(F.col("producto"))) > 0)
        .filter(F.col("cantidad") > 0)
        .filter(F.col("precio_unitario") > 0)
        .withColumn(
            "total_venta",
            (F.col("cantidad") * F.col("precio_unitario")).cast("decimal(14,2)"),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="pipeline_ventas_por_categoria",
    comment="Valor vendido y cantidad de ventas por categoría.",
)
def pipeline_ventas_por_categoria():
    return (
        dp.read("pipeline_ventas_limpias")
        .groupBy("categoria")
        .agg(
            F.sum("total_venta")
            .cast("decimal(16,2)")
            .alias("valor_total_vendido"),
            F.count("*").alias("cantidad_ventas"),
        )
        .orderBy("categoria")
    )
