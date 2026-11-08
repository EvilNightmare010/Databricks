# Taller 5 - Databricks

**Estudiante:** Juan José Ramírez Pulgarín  
**Identificador usado en Databricks:** `jjrp`  
**Asignatura:** Ingeniería de Datos  
**Fecha:** 7/11/2026  
**Plataforma:** Databricks Free Edition con cómputo serverless

## 1. Objetivo

Construir un flujo de datos para una tienda: almacenar ventas en una tabla Delta Bronze, aplicar reglas de calidad para producir tablas Silver y de resumen, automatizar la ejecución mediante un job y definir las transformaciones mediante Lakeflow Spark Declarative Pipelines.

## 2. Estructura del repositorio

```text
Taller_Databricks/
├── README.md
├── notebooks/
│   ├── 01_cargar_ventas.py
│   ├── 02_transformar_ventas.py
│   └── 03_consultar_ventas.py
├── pipeline/
│   └── pipeline_ventas.py
├── sql/
│   └── validaciones.sql
└── evidencias/
    └── .gitkeep
```

Los archivos `.py` están escritos en formato fuente de notebook de Databricks. Se pueden importar desde el workspace y ejecutar con cómputo serverless.

> **Compatibilidad con Serverless:** `precio_unitario` se construye inicialmente como `DOUBLE` porque los literales del conjunto de Python son números `float`. Después de crear el DataFrame, la columna se convierte explícitamente a `DECIMAL(12,2)`. Así se conserva el tipo requerido en la tabla Delta y se evita el error de conversión de `DecimalType` durante `createDataFrame` en Spark Connect.

## 3. Configuración inicial

En Databricks, identifica el catálogo habilitado en el explorador de Catalog. En los archivos de este repositorio, reemplaza el valor de `CATALOG` si el catálogo de tu workspace no se llama `workspace`.

La solución utiliza:

```text
CATALOG = workspace
SCHEMA = ingenieria_datos_jjrp
```

El nombre completo de las tablas queda así:

```text
workspace.ingenieria_datos_jjrp.ventas_bronze
workspace.ingenieria_datos_jjrp.ventas_silver
workspace.ingenieria_datos_jjrp.ventas_por_categoria
```

Si tu catálogo tiene otro nombre, conserva el esquema `ingenieria_datos_jjrp` y cambia únicamente el catálogo en los notebooks, consultas y configuración del pipeline.

## 4. Datos de entrada

Se cargan 16 filas. El conjunto contiene intencionalmente:

- Una venta duplicada: `id_venta = 1005`.
- Una venta sin producto: `id_venta = 1009`.
- Una venta con cantidad negativa: `id_venta = 1010`.
- Una venta con precio unitario no válido: `id_venta = 1011`.

La tabla Bronze conserva los datos originales para permitir su auditoría. La tabla Silver elimina las filas idénticas duplicadas y descarta las ventas que incumplen las reglas de calidad.

## 5. Ejercicio 1: almacenar y consultar en el catálogo

### 5.1 Investigación: ¿qué es Unity Catalog?

Unity Catalog es la capa central de gobierno y descubrimiento de datos de Databricks. Organiza los objetos mediante una jerarquía de tres niveles: catálogo, esquema y objeto, por ejemplo `catalogo.esquema.tabla`. Además de facilitar la búsqueda de tablas, permite administrar permisos, propietarios, metadatos, linaje y auditoría desde un punto común. Esto evita que cada notebook tenga que administrar de forma aislada quién puede leer o modificar los datos.

### 5.2 Investigación: diferencia entre DataFrame de Spark y tabla Delta

Un DataFrame de Spark es una representación distribuida de datos en memoria lógica que vive dentro de la ejecución de un notebook o de una aplicación. Puede transformarse y consultarse, pero por sí solo no constituye un almacenamiento persistente. Una tabla Delta es un objeto persistente respaldado por archivos y por un registro transaccional Delta Lake; conserva el esquema, permite operaciones ACID, versionamiento y lecturas consistentes entre ejecuciones. En este taller, el DataFrame se usa para construir los datos y la tabla Delta `ventas_bronze` se usa para conservarlos y compartirlos entre notebooks.

### 5.3 Procedimiento

1. Ejecutar `notebooks/01_cargar_ventas.py`.
2. Abrir **Catalog** y localizar el catálogo, el esquema `ingenieria_datos_jjrp` y la tabla `ventas_bronze`.
3. Confirmar las columnas y tipos: `id_venta` entero, `fecha` fecha, `producto` texto, `categoria` texto, `cantidad` entero y `precio_unitario` decimal.
4. Importar y ejecutar el notebook de lectura solicitado por el ejercicio o ejecutar las consultas equivalentes de `sql/validaciones.sql`.

### 5.4 Resultado esperado

La tabla Bronze debe contener 16 registros, porque conserva tanto la fila duplicada como las filas inválidas.

## 6. Ejercicio 2: transformación y calidad

### 6.1 Reglas aplicadas

1. Eliminar registros idénticos duplicados.
2. Conservar únicamente filas con `producto` no nulo y no vacío.
3. Conservar únicamente filas con `cantidad > 0`.
4. Conservar únicamente filas con `precio_unitario > 0`.
5. Calcular `total_venta = cantidad * precio_unitario`.
6. Sobrescribir las tablas de salida para que la repetición del notebook sea idempotente y no acumule duplicados.

### 6.2 Resultado esperado

Partiendo de 16 filas Bronze:

- Se elimina una copia duplicada.
- Se descartan tres filas inválidas: producto ausente, cantidad negativa y precio no válido.
- `ventas_silver` debe quedar con 12 registros.
- `ventas_por_categoria` debe tener un registro por categoría válida.

Con los datos incluidos en este repositorio, el resultado esperado es:

| Categoría | Valor total vendido | Cantidad de ventas |
|---|---:|---:|
| Oficina | 2240000.00 | 3 |
| Papelería | 244000.00 | 3 |
| Tecnología | 8600000.00 | 6 |

El total general esperado de `ventas_silver` es `11084000.00`.

Los valores exactos se obtienen ejecutando las consultas del archivo `sql/validaciones.sql`, pues allí se calcula el resumen directamente sobre las tablas creadas en el workspace.

## 7. Ejercicio 3: job

### 7.1 Investigación: ¿qué es un job?

Un job de Databricks es una definición de ejecución automatizada que reúne una o más tareas, sus parámetros, el cómputo y las reglas de ejecución. Sirve para repetir un proceso de forma controlada, programada y observable, sin abrir manualmente cada notebook.

### 7.2 Investigación: ¿qué es una tarea y cómo se diferencia de un job?

Una tarea es una unidad de trabajo dentro de un job, por ejemplo ejecutar un notebook, una consulta SQL o una actividad de transformación. El job es el contenedor y orquestador completo; una ejecución del job puede incluir varias tareas y coordinar su orden, reintentos y resultados.

### 7.3 Investigación: ¿qué significa establecer dependencias?

Establecer una dependencia significa indicar que una tarea solo puede comenzar cuando otra tarea previa termina bajo una condición determinada. En este taller, la transformación depende de la carga y solo se ejecuta si `01_cargar_ventas` finaliza correctamente. Si la primera tarea falla, la segunda queda omitida o se marca como bloqueada por la dependencia.

### 7.4 Investigación: ¿qué información del historial identifica un fallo?

El historial permite revisar el estado general, el estado de cada tarea, la duración, la hora de inicio y finalización, el mensaje de error, los registros de salida y el enlace a los detalles de la ejecución. La causa se identifica principalmente en los detalles de la tarea fallida y en el error de la primera línea que no pudo ejecutarse; el estado de la tarea dependiente confirma el efecto de la dependencia.

### 7.5 Configuración

Crear el job `job_ventas_jjrp` con estas tareas:

| Tarea | Notebook | Dependencia |
|---|---|---|
| `cargar_ventas` | `01_cargar_ventas` | Ninguna |
| `transformar_ventas` | `02_transformar_ventas` | `cargar_ventas` con condición de éxito |

Para la ejecución fallida se debe introducir temporalmente una referencia a una columna inexistente en la primera tarea. Después de capturar el fallo, retirar la referencia, guardar el notebook y ejecutar nuevamente hasta obtener un resultado exitoso.

## 8. Ejercicio 4: pipeline declarativo

### 8.1 Investigación: ¿qué es un pipeline de datos?

Un pipeline de datos es una secuencia organizada que lleva datos desde una fuente hasta productos preparados para consumo. Sus etapas habituales son ingestión, validación, limpieza, transformación, enriquecimiento, agregación, publicación y monitoreo. La separación por etapas facilita rastrear qué ocurrió con los datos y detectar en qué punto aparece un error.

### 8.2 Investigación: ¿qué significa declarativo?

Un pipeline declarativo describe el resultado que se quiere obtener y las relaciones entre los conjuntos de datos, en lugar de ordenar paso a paso toda la ejecución. El motor calcula el grafo de dependencias, decide el orden de actualización y administra parte de la ejecución. El código de este repositorio declara una vista limpia y una vista agregada que depende de ella.

### 8.3 Investigación: job de notebooks frente a pipeline declarativo

Un job de notebooks orquesta actividades explícitas: ejecuta una tarea, espera su resultado y luego inicia otra según las dependencias configuradas. Un pipeline declarativo define tablas o vistas y sus relaciones; Databricks administra el grafo de datos y actualiza los resultados conforme a esas definiciones. Ambos pueden automatizar procesos, pero el job se centra en coordinar tareas y el pipeline en declarar productos de datos reproducibles.

### 8.4 Procedimiento

1. Crear un pipeline ETL en **Workflows > Lakeflow Declarative Pipelines**.
2. Seleccionar cómputo serverless.
3. Configurar el catálogo y el esquema de publicación.
4. Agregar `pipeline/pipeline_ventas.py` como archivo fuente.
5. Ejecutar el pipeline manualmente.
6. Revisar el grafo: `ventas_bronze` → `pipeline_ventas_limpias` → `pipeline_ventas_por_categoria`.
7. Insertar una venta válida directamente en `ventas_bronze`, sin ejecutar de nuevo el notebook de carga.
8. Ejecutar nuevamente el pipeline y comparar el resumen antes y después.

La venta adicional propuesta es:

```sql
INSERT INTO workspace.ingenieria_datos_jjrp.ventas_bronze
VALUES (1016, DATE '2026-11-07', 'Audífonos', 'Tecnología', 2, CAST(75000.00 AS DECIMAL(12,2)));
```

El valor agregado esperado es `150000.00`. El resumen de la categoría `Tecnología` debe aumentar exactamente en ese valor, siempre que la fila no exista previamente y que el pipeline haya leído la tabla Bronze actualizada.

## 9. Evidencias y nombres de capturas

Guardar las imágenes dentro de `evidencias/` con estos nombres:

| Archivo | Qué debe mostrar |
|---|---|
| `00_esquema_ingenieria_datos_jjrp_creado.png` | Esquema creado dentro de `workspace` |
| `01_catalogo_esquema_ventas_bronze.png` | Catálogo abierto con el esquema y `ventas_bronze` visible |
| `02_ventas_bronze_columnas_tipos.png` | Columnas y tipos de `ventas_bronze` |
| `03_carga_exitosa_16_registros.png` | Ejecución exitosa de `01_cargar_ventas` con 16 filas |
| `04_consulta_conteo_bronze.png` | Resultado del conteo de 16 registros |
| `05_consulta_categoria_bronze.png` | Ventas de la categoría Tecnología |
| `06_transformacion_conteos.png` | Conteos antes y después de la limpieza |
| `07_ventas_silver.png` | Esquema de `ventas_silver` |
| `08_resumen_ventas_categoria.png` | Tabla `ventas_por_categoria` |
| `09_job_dependencias.png` | Grafo o vista de las dos tareas y su dependencia |
| `10_job_ejecucion_exitosa_inicial.png` | Primera ejecución exitosa del job |
| `11_job_ejecucion_fallida.png` | Historial con la primera tarea fallida y la segunda bloqueada/omitida |
| `12_job_detalle_error.png` | Mensaje detallado de la columna inexistente |
| `13_job_ejecucion_exitosa_final.png` | Historial final exitoso con duración de cada tarea |
| `14_pipeline_grafo.png` | Grafo de dependencias del pipeline |
| `15_pipeline_resumen_antes.png` | Resumen antes de insertar la venta adicional |
| `16_venta_adicional_bronze.png` | Fila 1016 insertada directamente en Bronze |
| `17_pipeline_resumen_despues.png` | Resumen después de insertar la venta adicional |
| `18_comparacion_pipeline.png` | Comparación que evidencia el incremento de 150000.00 |

No se deben fabricar capturas. Cada imagen debe ser tomada desde la interfaz de Databricks y corresponder con el resultado real de la ejecución.

## 10. Cibergrafía

1. Databricks. **What is Unity Catalog?** https://docs.databricks.com/aws/en/data-governance/unity-catalog
2. Databricks. **What is Delta Lake?** https://docs.databricks.com/aws/en/delta/
3. Apache Spark. **Spark SQL, DataFrames and Datasets Guide.** https://spark.apache.org/docs/latest/sql-programming-guide.html
4. Databricks. **Lakeflow Jobs.** https://docs.databricks.com/aws/en/jobs/
5. Databricks. **Lakeflow Spark Declarative Pipelines.** https://docs.databricks.com/aws/en/ldp/

## 11. Lista de comprobación final

- [x] Se reemplazó el catálogo por el catálogo realmente disponible.
- [x] Se ejecutó correctamente `01_cargar_ventas`.
- [x] `ventas_bronze` contiene 16 registros antes de la prueba incremental.
- [x] Se verificaron columnas y tipos en Catalog.
- [x] `ventas_silver` contiene solo registros válidos y no duplicados.
- [x] `ventas_por_categoria` fue sobrescrita sin acumular registros al repetir el notebook.
- [x] El job tiene dos tareas y la dependencia está configurada.
- [x] Se capturó una ejecución fallida y se explicó el error.
- [x] Se capturó una ejecución inicial y una ejecución final exitosa.
- [x] El pipeline muestra las dos vistas materializadas y sus dependencias.
- [x] Se agregó una fila válida directamente a Bronze sin ejecutar nuevamente el notebook de carga.
- [x] El resumen posterior aumentó en el valor correcto.
- [x] Las capturas tienen los nombres definidos en este README.
- [x] Se actualizaron los resultados numéricos con los valores reales observados en Databricks.

## 12. Resultados finales observados

La ejecución inicial produjo 12 registros válidos en Silver:

| Categoría | Valor total vendido | Cantidad de ventas |
|---|---:|---:|
| Oficina | 2240000.00 | 3 |
| Papelería | 244000.00 | 3 |
| Tecnología | 8600000.00 | 6 |

Después de insertar directamente en Bronze la venta `1016` (`Audífonos`, categoría `Tecnología`, cantidad `2`, precio unitario `75000.00`) y actualizar el pipeline:

- `pipeline_ventas_limpias` produjo 13 registros.
- `pipeline_ventas_por_categoria` conservó tres categorías.
- Tecnología pasó a `8750000.00` y 7 ventas.
- El incremento fue `150000.00`, exactamente `2 * 75000.00`.

Por lo tanto, la actualización del pipeline coincidió con el valor de la venta agregada y se comprobó que la nueva fila fue leída desde Bronze sin ejecutar nuevamente el notebook de carga.
