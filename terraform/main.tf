terraform {
  backend "s3" {
    bucket     = "terraform-state-storage-586877430255"
    lock_table = "terraform-state-lock-586877430255"
    region     = "us-west-2"

    // THIS MUST BE UNIQUE
    key = "lazarette.tfstate"
  }
}

provider "aws" {
  region = "us-west-2"
}

data "aws_ssm_parameter" "eks_cluster_endpoint" {
  name = "/eks/av-cluster-endpoint"
}

provider "kubernetes" {
  host = data.aws_ssm_parameter.eks_cluster_endpoint.value
}

module "statefulset_dev" {
  source = "github.com/byuoitav/terraform//modules/kubernetes-statefulset"

  // required
  name                 = "lazarette-dev"
  image                = "docker.pkg.github.com/byuoitav/lazarette/lazarette"
  image_version        = "v0.2.0"
  container_port       = 8080
  repo_url             = "https://github.com/byuoitav/lazarette"
  storage_mount_path   = "/opt/lazarette"
  storage_request_size = "25Gi"

  // optional
  image_pull_secret = "github-docker-registry"
  public_urls       = ["lazarette-dev.av.byu.edu"]
  container_env     = {}
  container_args = [
    "--port", "8080",
    "--log-level", "2",
    "--persist-path", "/opt/lazarette/backup.db",
    "--persist-interval", "1m"
  ]
}

// TODO prod
