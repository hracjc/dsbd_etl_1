# Entrega 02

## Predicción

Antes de ejecutar el job se espera que la cobertura dependa del universo disponible en el Parquet y de la completitud de cada serie.

## Resultado

El proceso aún está pendiente de ejecución real. La salida prevista es `data/serving/spark/cobertura_decada/`.

## Decisiones

Se usa el indicador y la década como clave. Solo se cuentan valores no nulos, países distintos y rangos de años con dato.
Se estudiará el número de particiones y el efecto del shuffle al agrupar.

## Evidencia

La evidencia prevista son el event log de Spark y el fichero generado en el directorio de salida. Aún hay que copiar el subdirectorio real a `evidencias/`.

## País

El cálculo toma los países presentes en los datos, no una lista fija.

## Pendientes

- Instalar PySpark y ejecutar el job.
- Guardar y documentar el event log.
- Comparar el tamaño y lectura de CSV frente a Parquet.
