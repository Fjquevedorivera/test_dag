Pasos para montar el DAG en Cloud Composer.

1. Habilitar Cloud Composer API en el proyecto de Google Cloud.

2. Usar o crear un entorno de Composer en el proyecto (esto tarda unos 25 minutos).

3. Crear dos variables de entorno en la configuración de Composer:
   - `gcs_bucket_wom` con el ID del bucket del entorno.
   - `project_id_wom` con el ID del proyecto.
   
4. Subir la carpeta `file_upload` al directorio `dags` del bucket de Composer (tardará unos minutos en reflejarse en la UI de Airflow).

5. Ejecutar el DAG desde la UI de Airflow, que aparecerá como `gcs_to_bigquery`.

6. Verificar la creación del dataset y las tablas (`my_dataset`, `my_table`, `transformate_table`) en BigQuery.
