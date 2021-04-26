terraform {
  backend "remote" {
    organization = "airbyte"

    workspaces {
      name = "source-medium"
    }
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.64.0"
    }
  }
}
