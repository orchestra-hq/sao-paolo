import json

from google.api_core.exceptions import Forbidden, NotFound, Unauthorized
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from pydantic import ValidationError

from ..logger import log_info
from ..models import StateApiModel
from ..state_errors import StateLoadError, StateSaveError
from ..state_filters import apply_integration_account_filter
from .logging import log_state_loaded, log_state_saved


class GCSStateBackend:
    def __init__(self, bucket: str, key: str) -> None:
        self._bucket = bucket
        self._key = key

    def load(self) -> StateApiModel:
        bucket, key = self._bucket, self._key

        try:
            client = storage.Client()
        except DefaultCredentialsError as e:
            raise StateLoadError(
                f"GCS credentials not found. Configure Application Default Credentials "
                f"(e.g. `gcloud auth application-default login` or set "
                f"GOOGLE_APPLICATION_CREDENTIALS). Details: {e}"
            ) from e

        try:
            blob = client.bucket(bucket).blob(key)
            raw = blob.download_as_text(encoding="utf-8")
        except NotFound:
            # Distinguish missing blob (expected on first run) from missing bucket
            # (config error). Both return 404 from the GCS API, so verify the bucket
            # exists before treating this as an empty-state case.
            try:
                client.get_bucket(bucket)
            except NotFound as bucket_err:
                raise StateLoadError(
                    f"GCS bucket 'gs://{bucket}' does not exist: {bucket_err}"
                ) from bucket_err
            except (Forbidden, Unauthorized) as e:
                raise StateLoadError(
                    f"Permission denied reading gs://{bucket}/{key}. "
                    f"Ensure the service account has storage.objects.get permission: {e}"
                ) from e
            except Exception as e:
                raise StateLoadError(
                    f"Failed to load state from gs://{bucket}/{key}: {e}"
                ) from e
            log_info(
                f"No state object at gs://{bucket}/{key}; starting with empty state."
            )
            return StateApiModel(state={})
        except (Forbidden, Unauthorized) as e:
            raise StateLoadError(
                f"Permission denied reading gs://{bucket}/{key}. "
                f"Ensure the service account has storage.objects.get permission: {e}"
            ) from e
        except Exception as e:
            raise StateLoadError(
                f"Failed to load state from gs://{bucket}/{key}: {e}"
            ) from e

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise StateLoadError(
                f"State object at gs://{bucket}/{key} is not valid JSON: {e}"
            ) from e

        try:
            state = StateApiModel.model_validate(data)
        except (ValidationError, ValueError) as e:
            raise StateLoadError(
                f"State object at gs://{bucket}/{key} failed validation: {e}"
            ) from e

        apply_integration_account_filter(state)
        log_state_loaded("gcs", state)
        return state

    def save(self, state: StateApiModel) -> None:
        bucket, key = self._bucket, self._key

        try:
            client = storage.Client()
        except DefaultCredentialsError as e:
            raise StateSaveError(
                f"GCS credentials not found. Configure Application Default Credentials "
                f"(e.g. `gcloud auth application-default login` or set "
                f"GOOGLE_APPLICATION_CREDENTIALS). Details: {e}"
            ) from e

        payload = state.model_dump_json(exclude_none=True)
        try:
            blob = client.bucket(bucket).blob(key)
            blob.upload_from_string(payload, content_type="application/json; charset=utf-8")
        except (Forbidden, Unauthorized) as e:
            raise StateSaveError(
                f"Permission denied writing gs://{bucket}/{key}. "
                f"Ensure the service account has storage.objects.create permission: {e}"
            ) from e
        except Exception as e:
            raise StateSaveError(
                f"Failed to save state to gs://{bucket}/{key}: {e}"
            ) from e

        log_state_saved("gcs")
