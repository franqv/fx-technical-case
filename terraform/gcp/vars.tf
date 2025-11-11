# ID del proyecto de Google Cloud donde se desplegarán los recursos.
variable "gcp_project_id" {
  description = "The ID of the GCP project"
  type        = string
}

# Región por defecto para los recursos (ej: europe-west4)
variable "gcp_region" {
  description = "The region for the resources"
  type        = string
  default     = "europe-west1"
}

# Zona por defecto para recursos que requieran zonales (ej: europe-west4-a)
variable "gcp_zone" {
  description = "The zone for the resources"
  type        = string
  default     = "europe-west1-b"
}

# Nombre para el bucket de Cloud Storage
variable "storage_bucket_name" {
  description = "Unique name for the Cloud Storage bucket (must be globally unique)"
  type        = string
}

# Nombre para el BigQuery Dataset
variable "bq_dataset_id" {
  description = "The ID for the BigQuery Dataset"
  type        = string
  default     = "data_engineering_dataset"
}