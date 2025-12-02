"""
Power BI Semantic Model Partition Refresh Script
Refreshes all partitions for specified tables when triggered by external scheduler
Includes robust error handling, security best practices, and performance optimizations
"""

import msal
import requests
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmailNotifier:
    """Handles email notifications via Microsoft Graph API for Outlook/Microsoft 365"""

    GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0/me/sendMail"
    REQUEST_TIMEOUT = 30

    def __init__(self, enable_notifications: bool = True):
        """
        Initialize the email notifier

        Args:
            enable_notifications: Whether to enable email notifications
        """
        self.enable_notifications = enable_notifications
        self.access_token = None
        self.recipients = []

        if self.enable_notifications:
            self._initialize_email_config()

    def _initialize_email_config(self) -> None:
        """Retrieve email configuration from Azure Key Vault"""
        try:
            # Retrieve email settings from Key Vault
            # Expected format: comma-separated email addresses
            recipients_str = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-notification-recipients")
            self.recipients = [email.strip() for email in recipients_str.split(",") if email.strip()]

            # Retrieve Graph API credentials
            self.email_client_id = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-email-client-id")
            self.email_client_secret = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-email-client-secret")
            self.email_tenant_id = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-email-tenant-id")
            self.sender_email = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-sender-email")

            if not self.recipients:
                logger.warning("No email recipients configured. Notifications will be disabled.")
                self.enable_notifications = False
                return

            logger.info(f"Email notifications configured for {len(self.recipients)} recipient(s)")

        except Exception as e:
            logger.warning(f"Failed to retrieve email configuration: {str(e)}. Email notifications will be disabled.")
            self.enable_notifications = False

    def _get_secret(self, vault_name: str, secret_name: str) -> str:
        """
        Retrieve secret from Azure Key Vault

        Args:
            vault_name: Name of the Key Vault
            secret_name: Name of the secret to retrieve

        Returns:
            The secret value
        """
        try:
            secret = mssparkutils.credentials.getSecret(vault_name, secret_name)
            if not secret:
                raise ValueError(f"Secret '{secret_name}' is empty")
            return secret
        except Exception as e:
            logger.error(f"Error retrieving secret '{secret_name}': {str(e)}")
            raise

    def _acquire_graph_token(self) -> str:
        """
        Acquire OAuth2 access token for Microsoft Graph API

        Returns:
            Valid access token
        """
        try:
            authority_url = f"https://login.microsoftonline.com/{self.email_tenant_id}"
            scope = ["https://graph.microsoft.com/.default"]

            app = msal.ConfidentialClientApplication(
                self.email_client_id,
                authority=authority_url,
                client_credential=self.email_client_secret
            )

            result = app.acquire_token_for_client(scopes=scope)

            if 'access_token' not in result:
                error_desc = result.get('error_description', 'Unknown error')
                raise Exception(f"Token acquisition failed: {error_desc}")

            logger.info("Successfully acquired Graph API access token")
            return result['access_token']

        except Exception as e:
            logger.error(f"Failed to acquire Graph API token: {str(e)}")
            raise

    def _create_email_body(
        self,
        status: str,
        tables_refreshed: List[Dict[str, any]],
        message: str,
        error_details: Optional[str] = None
    ) -> str:
        """
        Create HTML email body with refresh details

        Args:
            status: Status of the refresh (Success/Failed)
            tables_refreshed: List of tables/partitions that were refreshed
            message: Status message
            error_details: Optional error details for failures

        Returns:
            HTML formatted email body
        """
        status_color = "#28a745" if status == "Success" else "#dc3545"
        status_icon = "✓" if status == "Success" else "✗"

        tables_html = "<ul>"
        for item in tables_refreshed:
            table_name = item.get('table', 'Unknown')
            partition = item.get('partition', 'All partitions')
            tables_html += f"<li><strong>{table_name}</strong>"
            if partition != 'All partitions':
                tables_html += f" - Partition: {partition}"
            tables_html += "</li>"
        tables_html += "</ul>"

        error_section = ""
        if error_details:
            error_section = f"""
            <div style="background-color: #f8d7da; border-left: 4px solid #dc3545; padding: 12px; margin-top: 15px;">
                <h3 style="color: #721c24; margin-top: 0;">Error Details:</h3>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{error_details}</pre>
            </div>
            """

        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: {status_color}; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
                .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-radius: 0 0 5px 5px; }}
                .info-box {{ background-color: white; padding: 15px; margin: 10px 0; border-radius: 5px; border: 1px solid #e0e0e0; }}
                h1 {{ margin: 0; font-size: 24px; }}
                h3 {{ color: #555; margin-top: 0; }}
                .timestamp {{ color: #666; font-size: 14px; margin-top: 10px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{status_icon} Power BI Refresh {status}</h1>
                    <div class="timestamp">Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
                </div>
                <div class="content">
                    <div class="info-box">
                        <h3>Status Message:</h3>
                        <p>{message}</p>
                    </div>
                    <div class="info-box">
                        <h3>Tables/Partitions Refreshed:</h3>
                        {tables_html}
                    </div>
                    {error_section}
                    <p style="color: #666; font-size: 12px; margin-top: 20px;">
                        This is an automated notification from the Power BI Partition Refresh service.
                    </p>
                </div>
            </div>
        </body>
        </html>
        """

        return html_body

    def send_notification(
        self,
        success: bool,
        tables_refreshed: List[Dict[str, any]],
        message: str,
        error_details: Optional[str] = None
    ) -> None:
        """
        Send email notification about refresh status

        Args:
            success: Whether the refresh was successful
            tables_refreshed: List of tables/partitions that were refreshed
            message: Status message
            error_details: Optional error details for failures
        """
        if not self.enable_notifications:
            logger.info("Email notifications are disabled. Skipping notification.")
            return

        try:
            # Acquire access token
            if not self.access_token:
                self.access_token = self._acquire_graph_token()

            # Prepare email content
            status = "Success" if success else "Failed"
            subject = f"Power BI Refresh {status} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            body_html = self._create_email_body(status, tables_refreshed, message, error_details)

            # Build recipients list
            to_recipients = [{"emailAddress": {"address": email}} for email in self.recipients]

            # Create email message
            email_message = {
                "message": {
                    "subject": subject,
                    "body": {
                        "contentType": "HTML",
                        "content": body_html
                    },
                    "toRecipients": to_recipients
                },
                "saveToSentItems": "true"
            }

            # Send email via Graph API
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }

            response = requests.post(
                self.GRAPH_API_ENDPOINT,
                headers=headers,
                json=email_message,
                timeout=self.REQUEST_TIMEOUT
            )

            response.raise_for_status()
            logger.info(f"Email notification sent successfully to {len(self.recipients)} recipient(s)")

        except Exception as e:
            # Don't fail the entire workflow if email fails
            logger.error(f"Failed to send email notification: {str(e)}")


class PowerBIRefreshManager:
    """Manages Power BI semantic model partition refreshes with error handling and security"""

    # Constants
    REQUEST_TIMEOUT = 30  # seconds
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2

    def __init__(self):
        """Initialize the refresh manager with secure credential retrieval"""
        self.access_token = None
        self.workspace_id = None
        self.dataset_id = None
        self._initialize_credentials()

    def _initialize_credentials(self) -> None:
        """Securely retrieve and validate credentials from Azure Key Vault"""
        try:
            # Retrieve secrets from Azure Key Vault
            self.client_id = self._get_secret("tfa-kv-auth-DAP-0001", "ta-DAP-SPrincipal01-id")
            self.client_secret = self._get_secret("tfa-kv-auth-DAP-0001", "ta-DAP-SPrincipal01-secret")
            self.tenant_id = self._get_secret("tfa-kv-auth-DAP-0001", "tenant-id")
            self.workspace_id = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-workspace-id-finans")
            self.dataset_id = self._get_secret("tfa-kv-auth-DAP-0001", "pbi-dataset-id-finans")

            # Validate all credentials are retrieved
            if not all([self.client_id, self.client_secret, self.tenant_id,
                       self.workspace_id, self.dataset_id]):
                raise ValueError("One or more required secrets are missing or empty")

            logger.info("Successfully retrieved all credentials from Key Vault")

        except Exception as e:
            logger.error(f"Failed to retrieve credentials: {str(e)}")
            raise

    def _get_secret(self, vault_name: str, secret_name: str) -> str:
        """
        Safely retrieve secret from Azure Key Vault with validation

        Args:
            vault_name: Name of the Key Vault
            secret_name: Name of the secret to retrieve

        Returns:
            The secret value

        Raises:
            ValueError: If secret is empty or None
        """
        try:
            secret = mssparkutils.credentials.getSecret(vault_name, secret_name)
            if not secret:
                raise ValueError(f"Secret '{secret_name}' is empty")
            return secret
        except Exception as e:
            logger.error(f"Error retrieving secret '{secret_name}': {str(e)}")
            raise

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry logic and connection pooling

        Returns:
            Configured requests Session object
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _acquire_access_token(self) -> str:
        """
        Acquire OAuth2 access token with validation

        Returns:
            Valid access token

        Raises:
            Exception: If token acquisition fails
        """
        try:
            authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"
            scope = ["https://analysis.windows.net/powerbi/api/.default"]

            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority_url,
                client_credential=self.client_secret
            )

            result = app.acquire_token_for_client(scopes=scope)

            # Validate token was acquired successfully
            if 'access_token' not in result:
                error_desc = result.get('error_description', 'Unknown error')
                error_code = result.get('error', 'Unknown')
                raise Exception(f"Token acquisition failed: {error_code} - {error_desc}")

            logger.info("Successfully acquired access token")
            return result['access_token']

        except Exception as e:
            logger.error(f"Failed to acquire access token: {str(e)}")
            raise

    def _get_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers with fresh access token

        Returns:
            Dictionary of HTTP headers
        """
        if not self.access_token:
            self.access_token = self._acquire_access_token()

        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

    def get_latest_refresh_status(self) -> Optional[str]:
        """
        Get the status of the most recent dataset refresh

        Returns:
            Status string (e.g., 'Completed', 'Failed', 'Unknown') or None if no refresh history
        """
        try:
            url = (f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/"
                   f"datasets/{self.dataset_id}/refreshes?$top=1")

            session = self._create_session()

            response = session.get(
                url=url,
                headers=self._get_headers(),
                timeout=self.REQUEST_TIMEOUT
            )

            # Check for HTTP errors
            response.raise_for_status()

            data = response.json()

            # Validate response structure
            if 'value' not in data or not data['value']:
                logger.warning("No refresh history found for dataset")
                return None

            status = data['value'][0].get('status', 'Unknown')
            logger.info(f"Latest refresh status: {status}")

            # Log additional details for failed refreshes
            if status == 'Failed':
                error_info = data['value'][0].get('serviceExceptionJson', 'No error details available')
                logger.error(f"Refresh failure details: {error_info}")

            return status

        except requests.exceptions.Timeout:
            logger.error("Request timeout while checking refresh status")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error while checking refresh status: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while checking refresh status: {str(e)}")
            raise

    def trigger_partition_refresh(
        self,
        tables_and_partitions: List[Dict[str, any]],
        commit_mode: str = "transactional",
        refresh_type: str = "full"
    ) -> Tuple[bool, str]:
        """
        Trigger a refresh for specific partitions

        Args:
            tables_and_partitions: List of dicts with 'table' and optional 'partition' keys
                Example: [{"table": "Finanspostering", "partition": "2025Q206"}]
            commit_mode: Either "transactional" or "partialBatch"
            refresh_type: Either "full", "automatic", "dataOnly", "calculate", or "clearValues"

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Validate inputs
            if not tables_and_partitions:
                raise ValueError("tables_and_partitions cannot be empty")

            if commit_mode not in ["transactional", "partialBatch"]:
                raise ValueError(f"Invalid commit_mode: {commit_mode}")

            valid_types = ["full", "automatic", "dataOnly", "calculate", "clearValues"]
            if refresh_type not in valid_types:
                raise ValueError(f"Invalid refresh_type: {refresh_type}")

            # Build refresh request body
            body = {
                "type": refresh_type,
                "commitMode": commit_mode,
                "objects": tables_and_partitions,
                "applyRefreshPolicy": False
            }

            # Correct endpoint for POST refresh (without /refreshes?$top=1)
            url = (f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/"
                   f"datasets/{self.dataset_id}/refreshes")

            logger.info(f"Triggering refresh for {len(tables_and_partitions)} table(s)/partition(s)")
            logger.debug(f"Refresh body: {json.dumps(body, indent=2)}")

            session = self._create_session()

            response = session.post(
                url=url,
                headers=self._get_headers(),
                json=body,
                timeout=self.REQUEST_TIMEOUT
            )

            # Check for HTTP errors
            response.raise_for_status()

            # 202 Accepted is the expected response for async refresh operations
            if response.status_code == 202:
                logger.info("Refresh request accepted successfully")
                return True, "Refresh triggered successfully"
            else:
                logger.warning(f"Unexpected status code: {response.status_code}")
                return True, f"Refresh triggered with status code: {response.status_code}"

        except requests.exceptions.Timeout:
            error_msg = "Request timeout while triggering refresh"
            logger.error(error_msg)
            return False, error_msg
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error while triggering refresh: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error while triggering refresh: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def safe_refresh_workflow(
        self,
        tables_and_partitions: List[Dict[str, any]],
        email_notifier: Optional[EmailNotifier] = None
    ) -> None:
        """
        Execute a safe refresh workflow with status checking and email notifications

        Args:
            tables_and_partitions: List of tables/partitions to refresh
            email_notifier: Optional EmailNotifier instance for sending notifications
        """
        error_details = None
        try:
            # Get latest refresh status
            logger.info("Checking latest refresh status...")
            status = self.get_latest_refresh_status()

            # Handle different status scenarios
            if status is None:
                logger.info("No previous refresh found. Proceeding with refresh.")
                print("ℹ️  No previous refresh history found.")
            elif status == "Unknown":
                logger.warning("Semantic model is currently refreshing. Aborting to avoid conflicts.")
                print("⚠️  Semantic model is currently refreshing. Please try again later.")
                return
            elif status == "Disabled":
                logger.error("Refresh is disabled for this dataset")
                print("❌ Refresh is disabled for this dataset. Please check dataset settings.")
                return
            elif status == "Failed":
                logger.warning("Previous refresh failed. Proceeding with new refresh attempt.")
                print("⚠️  Previous refresh failed. Attempting new refresh...")
            elif status == "Completed":
                logger.info("Previous refresh completed successfully. Proceeding with new refresh.")
                print("✓ Previous refresh completed successfully.")
            else:
                logger.warning(f"Unknown status: {status}. Proceeding cautiously.")
                print(f"⚠️  Unknown status: {status}")

            # Trigger the refresh
            print(f"🔄 Triggering refresh for {len(tables_and_partitions)} table(s)/partition(s)...")
            success, message = self.trigger_partition_refresh(tables_and_partitions)

            if success:
                print(f"✓ {message}")
                logger.info("Refresh workflow completed successfully")

                # Send success notification
                if email_notifier:
                    email_notifier.send_notification(
                        success=True,
                        tables_refreshed=tables_and_partitions,
                        message=message
                    )
            else:
                print(f"❌ {message}")
                logger.error("Refresh workflow failed")
                error_details = message

                # Send failure notification
                if email_notifier:
                    email_notifier.send_notification(
                        success=False,
                        tables_refreshed=tables_and_partitions,
                        message=message,
                        error_details=error_details
                    )

        except Exception as e:
            error_msg = f"Error in refresh workflow: {str(e)}"
            logger.error(error_msg, exc_info=True)
            print(f"❌ Fatal error: {str(e)}")

            # Send failure notification for exceptions
            if email_notifier:
                email_notifier.send_notification(
                    success=False,
                    tables_refreshed=tables_and_partitions,
                    message="Fatal error occurred during refresh workflow",
                    error_details=str(e)
                )
            raise


def main():
    """Main execution function"""
    try:
        # Initialize the refresh manager
        manager = PowerBIRefreshManager()

        # Initialize email notifier (set to False to disable notifications)
        email_notifier = EmailNotifier(enable_notifications=True)

        # Define tables and partitions to refresh
        # For all partitions in a table, omit the 'partition' key
        # For specific partitions, include the 'partition' key
        tables_to_refresh = [
            {
                "table": "Finanspostering"
                # Uncomment and modify to target specific partition(s):
                # "partition": "2025Q206"
            }
            # Add more tables/partitions as needed:
            # {"table": "AnotherTable", "partition": "2025Q301"}
        ]

        # Execute the safe refresh workflow with email notifications
        manager.safe_refresh_workflow(
            tables_and_partitions=tables_to_refresh,
            email_notifier=email_notifier
        )

    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)
        print(f"❌ Critical error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
