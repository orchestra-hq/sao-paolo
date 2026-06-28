import json
import os

from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from pydantic import ValidationError

from ..logger import log_info
from ..models import StateApiModel
from ..state_errors import StateLoadError, StateSaveError
from ..state_filters import apply_integration_account_filter
from .logging import log_state_loaded, log_state_saved


class AzureStateBackend:
    def __init__(self, account: str, container: str, key: str) -> None:
        self._account = account
        self._container = container
        self._key = key

    def _get_client(self) -> BlobServiceClient:
        # The clients construct lazily and do not authenticate until a blob
        # operation runs, so credential failures surface in load()/save() rather
        # than here.
        conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if conn_str:
            return BlobServiceClient.from_connection_string(conn_str)
        return BlobServiceClient(
            f"https://{self._account}.blob.core.windows.net",
            credential=DefaultAzureCredential(),
        )

    def load(self) -> StateApiModel:
        account, container, key = self._account, self._container, self._key
        uri = f"abfss://{container}@{account}.dfs.core.windows.net/{key}"

        client = self._get_client()

        try:
            blob_client = client.get_blob_client(container=container, blob=key)
            download = blob_client.download_blob()
            raw = download.readall().decode("utf-8")
        except ResourceNotFoundError:
            try:
                container_exists = client.get_container_client(container).exists()
            except Exception as e:
                raise StateLoadError(
                    f"Failed to check container '{container}' in account '{account}': {e}"
                ) from e
            if not container_exists:
                raise StateLoadError(
                    f"Azure container '{container}' in account '{account}' does not exist."
                )
            log_info(f"No state blob at {uri}; starting with empty state.")
            return StateApiModel(state={})
        except ClientAuthenticationError as e:
            # Must precede HttpResponseError: ClientAuthenticationError is a subclass.
            raise StateLoadError(
                f"Azure credentials not found or invalid while reading {uri}. "
                f"Run `az login` or set AZURE_STORAGE_CONNECTION_STRING. Details: {e}"
            ) from e
        except HttpResponseError as e:
            raise StateLoadError(
                f"Permission denied reading {uri}. "
                f"Ensure the identity has the 'Storage Blob Data Reader' role: {e}"
            ) from e
        except Exception as e:
            raise StateLoadError(f"Failed to load state from {uri}: {e}") from e

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise StateLoadError(f"State blob at {uri} is not valid JSON: {e}") from e

        try:
            state = StateApiModel.model_validate(data)
        except (ValidationError, ValueError) as e:
            raise StateLoadError(
                f"State blob at {uri} failed validation: {e}"
            ) from e

        apply_integration_account_filter(state)
        log_state_loaded("azure", state)
        return state

    def save(self, state: StateApiModel) -> None:
        account, container, key = self._account, self._container, self._key
        uri = f"abfss://{container}@{account}.dfs.core.windows.net/{key}"

        client = self._get_client()

        payload = state.model_dump_json(exclude_none=True).encode("utf-8")
        try:
            blob_client = client.get_blob_client(container=container, blob=key)
            blob_client.upload_blob(
                payload,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type="application/json; charset=utf-8"
                ),
            )
        except ClientAuthenticationError as e:
            # Must precede HttpResponseError: ClientAuthenticationError is a subclass.
            raise StateSaveError(
                f"Azure credentials not found or invalid while writing {uri}. "
                f"Run `az login` or set AZURE_STORAGE_CONNECTION_STRING. Details: {e}"
            ) from e
        except HttpResponseError as e:
            raise StateSaveError(
                f"Permission denied writing {uri}. "
                f"Ensure the identity has the 'Storage Blob Data Contributor' role: {e}"
            ) from e
        except Exception as e:
            raise StateSaveError(f"Failed to save state to {uri}: {e}") from e

        log_state_saved("azure")
