provider "google" {
  project     = var.name
  credentials = var.credentials
}

module "instance" {
  source = "./instance"
}
