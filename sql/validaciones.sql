-- Cambiar workspace por el catálogo habilitado en la cuenta de Databricks.
USE CATALOG workspace;
USE SCHEMA ingenieria_datos_jjrp;

-- Ejercicio 1: conteo total Bronze.
SELECT COUNT(*) AS registros_bronze
FROM ventas_bronze;

-- Ejercicio 1: consulta de una categoría.
SELECT *
FROM ventas_bronze
WHERE categoria = 'Tecnología'
ORDER BY id_venta;

-- Ejercicio 2: conteos antes y después.
SELECT
  (SELECT COUNT(*) FROM ventas_bronze) AS registros_antes,
  (SELECT COUNT(*) FROM ventas_silver) AS registros_despues;

-- Ejercicio 2: resumen por categoría.
SELECT *
FROM ventas_por_categoria
ORDER BY categoria;

-- Control de duplicados en Silver.
SELECT id_venta, fecha, producto, categoria, cantidad, precio_unitario, COUNT(*) AS repeticiones
FROM ventas_silver
GROUP BY id_venta, fecha, producto, categoria, cantidad, precio_unitario
HAVING COUNT(*) > 1;

-- Venta adicional para la prueba del pipeline.
-- Ejecutar solo después de guardar la evidencia del resumen inicial.
-- INSERT INTO workspace.ingenieria_datos_jjrp.ventas_bronze
-- VALUES (1016, DATE '2026-11-07', 'Audífonos', 'Tecnología', 2, CAST(75000.00 AS DECIMAL(12,2)));

-- Comparación posterior a la ejecución del pipeline.
SELECT *
FROM pipeline_ventas_por_categoria
WHERE categoria = 'Tecnología';
