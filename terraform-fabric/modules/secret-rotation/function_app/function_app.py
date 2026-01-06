"""
Azure Function for automatic secret rotation of Fabric service principal.

This function handles two triggers:
1. Event Grid trigger: Responds to SecretNearExpiry events from Key Vault
2. Timer trigger: Proactively checks and rotates secrets daily
"""

import os
import logging
from datetime import datetime, timedelta, timezone

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.graphrbac import GraphRbacManagementClient
from msgraph import GraphServiceClient
from msgraph.generated.applications.item.add_password.add_password_post_request_body import (
    AddPasswordPostRequestBody,
)
from msgraph.generated.models.password_credential import PasswordCredential

app = func.FunctionApp()

# Configuration from environment
KEY_VAULT_NAME = os.environ.get("KEY_VAULT_NAME")
FABRIC_SP_APP_ID = os.environ.get("FABRIC_SP_APP_ID")
SECRET_VALIDITY_DAYS = int(os.environ.get("SECRET_VALIDITY_DAYS", "90"))
ROTATION_DAYS_BEFORE = int(os.environ.get("ROTATION_DAYS_BEFORE", "30"))
SECRET_NAME = os.environ.get("SECRET_NAME_IN_KEYVAULT", "fabric-client-secret")
TENANT_ID = os.environ.get("AZURE_TENANT_ID")


def get_credential():
    """Get Azure credential using managed identity."""
    return DefaultAzureCredential()


def get_keyvault_client():
    """Get Key Vault secret client."""
    vault_url = f"https://{KEY_VAULT_NAME}.vault.azure.net"
    return SecretClient(vault_url=vault_url, credential=get_credential())


def get_graph_client():
    """Get Microsoft Graph client."""
    credential = get_credential()
    scopes = ["https://graph.microsoft.com/.default"]
    return GraphServiceClient(credentials=credential, scopes=scopes)


async def rotate_service_principal_secret():
    """
    Rotate the service principal secret:
    1. Create a new secret in Azure AD
    2. Update the secret in Key Vault
    3. Optionally remove old secrets
    """
    logging.info(f"Starting secret rotation for app: {FABRIC_SP_APP_ID}")

    try:
        # Initialize clients
        kv_client = get_keyvault_client()
        graph_client = get_graph_client()

        # Calculate new secret expiry
        end_date = datetime.now(timezone.utc) + timedelta(days=SECRET_VALIDITY_DAYS)

        # Create new password credential for the application
        password_credential = PasswordCredential(
            display_name=f"Rotated-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
            end_date_time=end_date,
        )

        request_body = AddPasswordPostRequestBody(
            password_credential=password_credential
        )

        # Add new password to the application
        result = await graph_client.applications.by_application_id(
            FABRIC_SP_APP_ID
        ).add_password.post(request_body)

        new_secret = result.secret_text
        logging.info("Successfully created new secret in Azure AD")

        # Update the secret in Key Vault
        # Set expiration slightly before the actual Azure AD expiration
        kv_expiry = end_date - timedelta(days=ROTATION_DAYS_BEFORE)

        kv_client.set_secret(
            SECRET_NAME,
            new_secret,
            expires_on=kv_expiry,
            content_type="text/plain",
            tags={
                "rotated_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": end_date.isoformat(),
                "app_id": FABRIC_SP_APP_ID,
            },
        )

        logging.info(f"Successfully updated secret '{SECRET_NAME}' in Key Vault")

        return {
            "status": "success",
            "message": f"Secret rotated successfully. New expiry: {end_date.isoformat()}",
            "app_id": FABRIC_SP_APP_ID,
            "new_expiry": end_date.isoformat(),
        }

    except Exception as e:
        logging.error(f"Failed to rotate secret: {str(e)}")
        raise


def should_rotate_secret(kv_client) -> bool:
    """Check if the secret should be rotated based on expiry."""
    try:
        secret = kv_client.get_secret(SECRET_NAME)
        if secret.properties.expires_on:
            days_until_expiry = (
                secret.properties.expires_on - datetime.now(timezone.utc)
            ).days
            logging.info(f"Secret expires in {days_until_expiry} days")
            return days_until_expiry <= ROTATION_DAYS_BEFORE
        return False
    except Exception as e:
        logging.warning(f"Could not check secret expiry: {e}")
        return False


@app.function_name(name="rotate_secret")
@app.event_grid_trigger(arg_name="event")
async def rotate_secret_event_grid(event: func.EventGridEvent):
    """
    Event Grid trigger for Key Vault SecretNearExpiry events.
    Triggered automatically when a secret is about to expire.
    """
    logging.info(f"Event Grid trigger received: {event.event_type}")
    logging.info(f"Subject: {event.subject}")
    logging.info(f"Data: {event.get_json()}")

    # Verify this is for our secret
    if SECRET_NAME not in event.subject:
        logging.info(f"Event not for our secret ({SECRET_NAME}), skipping")
        return

    result = await rotate_service_principal_secret()
    logging.info(f"Rotation result: {result}")


@app.function_name(name="rotate_secret_timer")
@app.timer_trigger(schedule="0 0 2 * * *", arg_name="timer", run_on_startup=False)
async def rotate_secret_timer(timer: func.TimerRequest):
    """
    Timer trigger to proactively check and rotate secrets.
    Runs daily at 2 AM UTC.
    """
    logging.info("Timer trigger fired for proactive rotation check")

    if timer.past_due:
        logging.info("Timer is past due, running anyway")

    kv_client = get_keyvault_client()

    if should_rotate_secret(kv_client):
        logging.info("Secret needs rotation, initiating...")
        result = await rotate_service_principal_secret()
        logging.info(f"Rotation result: {result}")
    else:
        logging.info("Secret does not need rotation yet")


@app.function_name(name="health_check")
@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    return func.HttpResponse(
        '{"status": "healthy", "service": "secret-rotation"}',
        mimetype="application/json",
        status_code=200,
    )
