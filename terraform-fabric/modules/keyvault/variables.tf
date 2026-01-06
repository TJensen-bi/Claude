variable "project_name" {
  description = "Project name used in resource naming"
  type        = string
  default     = "fabric"
}

variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "Environment must be dev, test, or prod."
  }
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "westeurope"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

###########################################
# Service Principal Configuration
###########################################

variable "fabric_tenant_id" {
  description = "Azure AD tenant ID for Fabric service principal"
  type        = string
  sensitive   = true
}

variable "fabric_client_id" {
  description = "Client ID of the Fabric service principal"
  type        = string
  sensitive   = true
}

variable "fabric_client_secret" {
  description = "Client secret of the Fabric service principal"
  type        = string
  sensitive   = true
}

variable "secret_expiration_date" {
  description = "Expiration date for the client secret (RFC3339 format)"
  type        = string
  default     = null
}

###########################################
# Access Control
###########################################

variable "deployer_principal_id" {
  description = "Principal ID of the service principal used for deployments"
  type        = string
}

variable "github_oidc_principal_id" {
  description = "Principal ID of the GitHub OIDC service principal"
  type        = string
}

variable "rotation_function_principal_id" {
  description = "Principal ID of the rotation Azure Function's managed identity"
  type        = string
  default     = null
}

###########################################
# Network Security
###########################################

variable "allowed_ip_ranges" {
  description = "List of IP ranges allowed to access Key Vault"
  type        = list(string)
  default     = []
}

variable "allowed_subnet_ids" {
  description = "List of subnet IDs allowed to access Key Vault"
  type        = list(string)
  default     = []
}

###########################################
# Rotation Configuration
###########################################

variable "enable_automatic_rotation" {
  description = "Enable automatic secret rotation via Azure Function"
  type        = bool
  default     = false
}

###########################################
# Monitoring
###########################################

variable "log_analytics_workspace_id" {
  description = "Log Analytics workspace ID for diagnostics"
  type        = string
  default     = null
}
