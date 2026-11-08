# Databricks notebook source
# MAGIC %md
# MAGIC # 02_transformar_ventas
# MAGIC
# MAGIC Limpieza de Bronze y generación de Silver y del resumen por categoría.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "ingenieria_datos_jjrp"
TABLE_BRONZE = f"{CATALOG}.{SCHEMA}.ventas_bronze"
TABLE_SILVER = f"{CATALOG}.{SCHEMA}.ventas_silver"
TABLE_CATEGORY = f"{CATALOG}.{SCHEMA}.ventas_por_categoria"

df_bronze = spark.table(TABLE_BRONZE)
conteo_bronze = df_bronze.count()

# dropDuplicates elimina filas idénticas completas, conservando el registro original.
df_silver = (
    df_bronze.dropDuplicates()
    .filter(F.col("producto").isNotNull())
    .filter(F.length(F.trim(F.col("producto"))) > 0)
    .filter(F.col("cantidad") > 0)
    .filter(F.col("precio_unitario") > 0)
    .withColumn(
        "total_venta",
        (F.col("cantidad") * F.col("precio_unitario")).cast("decimal(14,2)"),
    )
)

conteo_silver = df_silver.count()

df_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE_SILVER)

df_category = (
    df_silver.groupBy("categoria")
    .agg(
        F.sum("total_venta")
        .cast("decimal(16,2)")
        .alias("valor_total_vendido"),
        F.count("*").alias("cantidad_ventas"),
    )
    .orderBy("categoria")
)

df_category.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE_CATEGORY)

print(f"Registros antes de la limpieza: {conteo_bronze}")
print(f"Registros después de la limpieza: {conteo_silver}")
display(spark.table(TABLE_SILVER).orderBy("id_venta"))
display(spark.table(TABLE_CATEGORY))
