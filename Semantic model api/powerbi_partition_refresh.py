import msal
import requests
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging

"""
Denne del konfigurerer hvordan logningssystemet skal registrere beskeder med et bestemt format og niveau, og opretter en logger, der kan bruges til at logge beskeder i programmet. 
"""
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PowerBIRefreshManager:
    """Håndtering af Finans modellens partition processering"""

    # Constants
    REQUEST_TIMEOUT = 30  # seconds
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2

    def __init__(self):
        """Initialiser opdateringshåndteringen med hentning af legitimationsoplysninger"""
        self.access_token = None
        self.workspace_id = None
        self.dataset_id = None
        self._initialize_credentials()

    def _initialize_credentials(self) -> None:
        """Sikkert hente og validere credentials fra AKV"""
        try:
            # Hente secrets fra Azure Key Vault
            self.client_id = self._get_secret("dfa-kv-auth-DAP-0001", "da-DAP-SPrincipal01-id")
            self.client_secret = self._get_secret("dfa-kv-auth-DAP-0001", "da-DAP-SPrincipal01-secret")
            self.tenant_id = self._get_secret("dfa-kv-auth-DAP-0001", "tenant-id")
            self.workspace_id = self._get_secret("dfa-kv-auth-DAP-0001", "pbi-workspace-id-finans")
            self.dataset_id = self._get_secret("dfa-kv-auth-DAP-0001", "pbi-dataset-id-finans")

            # Valider om alle credentials er modtaget
            if not all([self.client_id, self.client_secret, self.tenant_id,
                       self.workspace_id, self.dataset_id]):
                raise ValueError("En eller flere secrets er ikke modtaget")

            logger.info("Alle credentials er hentet fra AKV")

        except Exception as e:
            logger.error(f"Fejlet i at hente credentials: {str(e)}")
            raise

    def _get_secret(self, vault_name: str, secret_name: str) -> str:
        """
        Henter secrets fra AKV med validering

        Args:
            vault_name: Navnet på en AKV
            secret_name: Navnet på en secret

        Return:
            Secret værdien

        Raises:
            ValueError: Hvis secret er tom eller none
        """
        try:
            secret = mssparkutils.credentials.getSecret(vault_name, secret_name)
            if not secret:
                raise ValueError(f"Secret '{secret_name}' er tom")
            return secret
        except Exception as e:
            logger.error(f"Fejl vedrørende secret '{secret_name}': {str(e)}")
            raise

    def _create_session(self) -> requests.Session:
        """
        opretter en HTTP-session med retry-logik og forbindelsespuljering, hvilket forbedrer ydeevnen. 
        Funktionen sikrer, at forespørgsler automatisk forsøges igen i tilfælde af visse fejl, og at forbindelser genbruges effektivt.
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
        Henter og validerer en OAuth2-adgangstoken. 
        Funktionen sikrer, at token hentes korrekt og håndterer eventuelle fejl, der opstår under processen, ved at logge og genkaste undtagelser.
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
        Sikrer, at en adgangstoken er tilgængelig og genererer de nødvendige HTTP-headers til autentificerede API-forespørgsler. 
        Foretager håndterering vedrørende hentning af adgangstokenen, hvis den ikke allerede er tilgængelig, og returnerer en korrekt konfigureret header-ordbog.
        """
        if not self.access_token:
            self.access_token = self._acquire_access_token()

        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

    def get_latest_refresh_status(self) -> Optional[str]:
        """
        Henter status fra den seneste data refresh.

        Returns:
            Status string (e.g., 'Completed', 'Failed', 'Unknown') or None hvis der ikke har været nogle tidligere refresh.
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
                logger.warning("Ingen tidligere refresh historie for Finans")
                return None

            status = data['value'][0].get('status', 'Unknown')
            logger.info(f"Sidste refresh status: {status}")

            # Log additional details for failed refreshes
            if status == 'Failed':
                error_info = data['value'][0].get('serviceExceptionJson', 'No error details available')
                logger.error(f"Refresh fejl: {error_info}")

            return status

        except requests.exceptions.Timeout:
            logger.error("Request er gået i timeout mens tjek for refresh status")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP fejl mens der blev tjekket for refresh status: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Uventet fejl mens der blev tjekket for refresh status: {str(e)}")
            raise

    def trigger_partition_refresh(
        self,
        tables_and_partitions: List[Dict[str, any]],
        commit_mode: str = "transactional",
        refresh_type: str = "full"
    ) -> Tuple[bool, str]:
        """
        Trigger en specifik partition

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
                raise ValueError("Tabeller og partitioner må ikke være tomme")

            if commit_mode not in ["transactional", "partialBatch"]:
                raise ValueError(f"Ugyldig commit_mode: {commit_mode}")

            valid_types = ["full", "automatic", "dataOnly", "calculate", "clearValues"]
            if refresh_type not in valid_types:
                raise ValueError(f"Ugyldig refresh_type: {refresh_type}")

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

            logger.info(f"Aktivere refresh for {len(tables_and_partitions)} table(s)/partition(s)")
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
                logger.info("Refresh request er accepteret successfuldt")
                return True, "Refresh er aktiveret successfuldt"
            else:
                logger.warning(f"Uventet status kode: {response.status_code}")
                return True, f"Refresh triggered med status kode: {response.status_code}"

        except requests.exceptions.Timeout:
            error_msg = "Din request er gået i timeout mens den forsøgte at trigger refresh"
            logger.error(error_msg)
            return False, error_msg
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP fejl er opstået mens den forsøgte at trigger refresh: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Uventet fejl mens den forsøgte at trigger refresh: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def safe_refresh_workflow(
        self,
        tables_and_partitions: List[Dict[str, any]]
    ) -> None:
        """
        Aktivering af refresh med statuscheck

        Args:
            tables_and_partitions: List of tables/partitions to refresh
        """
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
            print(f"🔄 Aktivering af refresh for {len(tables_and_partitions)} table(s)/partition(s)...")
            success, message = self.trigger_partition_refresh(tables_and_partitions)

            if success:
                print(f"✓ {message}")
                logger.info("Refresh workflow er udført")
            else:
                print(f"❌ {message}")
                logger.error("Refresh workflow er fejlet")

        except Exception as e:
            logger.error(f"Fejl i refresh workflow: {str(e)}", exc_info=True)
            print(f"❌ Fatal fejl: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        # Initialize the refresh manager
        manager = PowerBIRefreshManager()

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

        # Execute the safe refresh workflow
        manager.safe_refresh_workflow(tables_and_partitions=tables_to_refresh)

    except Exception as e:
        logger.critical(f"Kritisk fejl under main eksekvering: {str(e)}", exc_info=True)
        print(f"❌ Kritisk fejl: {str(e)}")
        raise


if __name__ == "__main__":
    main()
