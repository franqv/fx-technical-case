# ----------------------------------------------------
# RECURSOS DE INFRAESTRUCTURA PARA DATA ENGINEERING
# ----------------------------------------------------

# 1. Google Cloud Storage Bucket para almacenar datos raw y procesados (Data Lake)
resource "google_storage_bucket" "data_lake_bucket" {
  # El nombre debe ser único globalmente.
  name          = var.storage_bucket_name
  location      = upper(var.gcp_region) # La ubicación del bucket es una región o multiregión.
  force_destroy = true # Permite borrar el bucket incluso si tiene contenido (¡cuidado en producción!)
  uniform_bucket_level_access = true # Recomendado para seguridad y consistencia

  labels = {
    environment = "dev"
    owner       = "francisco"
    service     = "data_lake"
  }
}

# 2. BigQuery Dataset para data warehouse
resource "google_bigquery_dataset" "data_warehouse_dataset" {
  dataset_id                  = var.bq_dataset_id
  friendly_name               = "Data Engineering Warehouse"
  description                 = "Dataset para alojar las tablas del data warehouse."
  location                    = upper(var.gcp_region)

  # Configuración de expiración por defecto para tablas (opcional)
  default_table_expiration_ms = 3600000 # 1 hora (ejemplo)
}

# ----------------------------------------------------
# OUTPUTS
# ----------------------------------------------------

# Muestra el nombre del bucket de Storage creado
output "storage_bucket_url" {
  description = "URL del Cloud Storage Bucket creado"
  value       = google_storage_bucket.data_lake_bucket.url
}

# Muestra el ID del dataset de BigQuery creado
output "bigquery_dataset_id" {
  description = "ID del BigQuery Dataset creado"
  value       = google_bigquery_dataset.data_warehouse_dataset.dataset_id
}