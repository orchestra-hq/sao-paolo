import json

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import ValidationError

from ..logger import log_info
from ..models import StateApiModel
from ..state_errors import StateLoadError, StateSaveError
from ..state_filters import apply_integration_account_filter


class S3StateBackend:
    def __init__(self, bucket: str, key: str) -> None:
        self._bucket = bucket
        self._key = key

    def load(self) -> StateApiModel:
        bucket, key = self._bucket, self._key
        client = boto3.client("s3")

        try:
            response = client.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                log_info(
                    f"No state object at s3://{bucket}/{key}; starting with empty state."
                )
                return StateApiModel(state={})
            raise StateLoadError(
                f"Failed to load state from s3://{bucket}/{key}: {e}"
            ) from e

        try:
            raw = response["Body"].read().decode("utf-8")
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise StateLoadError(
                f"State object at s3://{bucket}/{key} is not valid JSON: {e}"
            ) from e

        try:
            state = StateApiModel.model_validate(data)
        except (ValidationError, ValueError) as e:
            raise StateLoadError(
                f"State object at s3://{bucket}/{key} failed validation: {e}"
            ) from e

        apply_integration_account_filter(state)
        log_info(f"State loaded from S3. Retrieved {len(state.state)} items.")
        return state

    def save(self, state: StateApiModel) -> None:
        bucket, key = self._bucket, self._key
        client = boto3.client("s3")

        payload_bytes = state.model_dump_json(exclude_none=True).encode("utf-8")
        try:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=payload_bytes,
                ContentType="application/json; charset=utf-8",
            )
        except (ClientError, BotoCoreError, OSError) as e:
            raise StateSaveError(
                f"Failed to save state to s3://{bucket}/{key}: {e}"
            ) from e
        log_info("State saved to S3")
