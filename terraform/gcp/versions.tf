# Define la versión mínima de Terraform y los proveedores necesarios.
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

# Configuración del proveedor de Google Cloud.
# Asegúrate de haber autenticado tu CLI de Google Cloud (gcloud auth application-default login)
# o de tener configuradas las credenciales de servicio adecuadas.
provider "google" {
  project = var.gcp_project_id
  region  = var.    gcp_region
  zone    = var.gcp_zone
}