# Amazon S3 — Zona Raw
## Rol: Almacén central de datos sin procesar.
### Justificación:
- Es el estándar para data lakes en AWS.
- Versionamiento opcional para mantener historial.
- Durabilidad del 99.999999999%.
- Costos extremadamente bajos comparados con bases de datos.
- Permite activar eventos automáticamente cuando se sube un archivo.

# AWS Lambda — Orquestación por evento
## Rol: Detecta que se cargó un archivo nuevo y dispara el Glue Job.
### Justificación:
- No requiere servidores (serverless → costo = 0 cuando no se usa).
- Escala automáticamente.
- Integración nativa con eventos de S3.
- Ejecuta lógica ligera: validación de nombre, fecha, duplicados, etc.
- **Lambda evita tener una instancia EC2 o un scheduler cron dedicado, lo cual reduce costos y complejidad.**
# AWS Glue Job — Procesamiento ETL
## Rol: Limpieza, transformación, estandarización y creación de tablas.
### Justificación:
- Servicio serverless de ETL basado en Spark (ideal para Excel grandes).
- Genera catálogos automáticos para consulta posterior.
- Permite tener procesos reproducibles y versionados.
- Gran compatibilidad con PySpark, lo que facilita migración futura.
# AWS Glue Data Catalog — Metadatos / Tablas
## Rol: Registrar los esquemas y tablas generadas.
### Justificación:
- Permite consultar los datos estructurados sin moverlos (a través de crawlers).
- Athena lo usa directamente.
- Evita tener bases de datos adicionales.
- Uso mensual → muy bajo costo.
# Amazon Athena — Consulta sobre tablas
## Rol: Servicio que permite consultar los datos en S3 usando SQL.
### Justificación:
- Serverless → pagas solo por consulta.
- Costo bajo si los datos están particionados.
- No requiere clusters, ni administración de bases.
# Power BI / Quicksight — Visualización
## Rol: Consumo de información por parte del cliente (dashboards mensuales).
### Justificación:
- Se conecta directo a Athena, o a archivos procesados en S3.
- Permite actualizaciones programadas y dashboards profesionales.
- Costo bajo y buen soporte para empresas.
# Flujo de datos — Justificación del diseño
## Por qué ese orden y no otro?
- Permite mantener un Data Lake moderno siguiendo un ELT.
- Proceso accionado por eventos → elimina necesidad de cronjobs o servidores.
- Cada capa cumple la filosofía Raw → Procesado → Consumo.
- Minimiza costos al máximo (solo se paga por procesamiento mensual y consultas).
