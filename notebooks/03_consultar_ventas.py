# Databricks notebook source
# MAGIC %md
# MAGIC # 03_consultar_ventas
# MAGIC
# MAGIC Lectura de `ventas_bronze` mediante su nombre completo desde un notebook
# MAGIC diferente al notebook de carga.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "ingenieria_datos_jjrp"
TABLE_BRONZE = f"{CATALOG}.{SCHEMA}.ventas_bronze"

df_bronze = spark.table(TABLE_BRONZE)

print(f"Tabla consultada: {TABLE_BRONZE}")
print(f"Cantidad de registros: {df_bronze.count()}")

# Categoría elegida para la consulta solicitada en el ejercicio.
display(
    df_bronze.filter(F.col("categoria") == "Tecnología").orderBy("id_venta")
)
