# Databricks notebook source
# MAGIC %md
# MAGIC # 01_cargar_ventas
# MAGIC
# MAGIC Carga reproducible de ventas en la tabla Delta Bronze.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "ingenieria_datos_jjrp"
TABLE_BRONZE = f"{CATALOG}.{SCHEMA}.ventas_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ventas = [
    (1001, "2026-10-01", "Laptop", "Tecnología", 2, 2500000.00),
    (1002, "2026-10-02", "Mouse", "Tecnología", 5, 80000.00),
    (1003, "2026-10-03", "Teclado", "Tecnología", 3, 150000.00),
    (1004, "2026-10-04", "Monitor", "Tecnología", 2, 900000.00),
    (1005, "2026-10-05", "Silla ergonómica", "Oficina", 1, 850000.00),
    (1005, "2026-10-05", "Silla ergonómica", "Oficina", 1, 850000.00),
    (1006, "2026-10-06", "Escritorio", "Oficina", 1, 1200000.00),
    (1007, "2026-10-07", "Cuaderno", "Papelería", 10, 12000.00),
    (1008, "2026-10-08", "Bolígrafo", "Papelería", 20, 3500.00),
    (1009, "2026-10-09", None, "Papelería", 4, 5000.00),
    (1010, "2026-10-10", "Cámara web", "Tecnología", -1, 180000.00),
    (1011, "2026-10-11", "Lámpara", "Oficina", 2, 0.00),
    (1012, "2026-10-12", "Audífonos", "Tecnología", 4, 75000.00),
    (1013, "2026-10-13", "Archivador", "Oficina", 2, 95000.00),
    (1014, "2026-10-14", "Resaltador", "Papelería", 12, 4500.00),
    (1015, "2026-10-15", "Impresora", "Tecnología", 1, 650000.00),
]

schema = StructType(
    [
        StructField("id_venta", IntegerType(), False),
        StructField("fecha", StringType(), False),
        StructField("producto", StringType(), True),
        StructField("categoria", StringType(), False),
        StructField("cantidad", IntegerType(), False),
        StructField("precio_unitario", DoubleType(), False),
    ]
)

df_ventas = (
    spark.createDataFrame(ventas, schema=schema)
    .withColumn("fecha", F.to_date("fecha"))
    .withColumn("precio_unitario", F.col("precio_unitario").cast("decimal(12,2)"))
)

# Se reemplaza la tabla para que la carga sea reproducible al volver a ejecutarla.
df_ventas.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE_BRONZE)

display(spark.table(TABLE_BRONZE).orderBy("id_venta"))
print(f"Registros cargados en {TABLE_BRONZE}: {spark.table(TABLE_BRONZE).count()}")
