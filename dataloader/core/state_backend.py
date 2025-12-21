"""State backend protocol and implementations for persisting state."""

import asyncio
import json
from pathlib import Path
from typing import Any, Protocol

from dataloader.core.exceptions import StateError


class StateBackend(Protocol):
    """Protocol for state persistence backends."""

    def load(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe.

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state

        Raises:
            StateError: If loading fails
        """
        ...

    def save(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe.

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        ...

    async def load_async(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe (async).

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state

        Raises:
            StateError: If loading fails
        """
        ...

    async def save_async(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe (async).

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        ...


class LocalStateBackend:
    """Local file-based state backend.

    Stores state in JSON files under `.state/{recipe_name}.json`.
    Uses atomic writes (write to temp file, then rename) to prevent corruption.
    """

    def __init__(self, state_dir: str | Path = ".state"):
        """Initialize local state backend.

        Args:
            state_dir: Directory to store state files (default: `.state`)
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def load(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from local JSON file.

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if file doesn't exist)

        Raises:
            StateError: If file exists but cannot be read or parsed
        """
        state_file = self.state_dir / f"{recipe_name}.json"

        if not state_file.exists():
            return {}

        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except json.JSONDecodeError as e:
            raise StateError(
                f"Failed to parse state file {state_file}: {e}",
                context={"recipe_name": recipe_name, "state_file": str(state_file)},
            ) from e
        except OSError as e:
            raise StateError(
                f"Failed to read state file {state_file}: {e}",
                context={"recipe_name": recipe_name, "state_file": str(state_file)},
            ) from e

    def save(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to local JSON file using atomic write.

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        state_file = self.state_dir / f"{recipe_name}.json"
        temp_file = self.state_dir / f"{recipe_name}.json.tmp"

        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            temp_file.replace(state_file)
        except OSError as e:
            if temp_file.exists():
                temp_file.unlink()
            raise StateError(
                f"Failed to save state file {state_file}: {e}",
                context={"recipe_name": recipe_name, "state_file": str(state_file)},
            ) from e
        except (TypeError, ValueError) as e:
            if temp_file.exists():
                temp_file.unlink()
            raise StateError(
                f"Failed to serialize state for {recipe_name}: {e}",
                context={"recipe_name": recipe_name},
            ) from e

    async def load_async(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from local JSON file (async).

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if file doesn't exist)

        Raises:
            StateError: If file exists but cannot be read or parsed
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.load, recipe_name)

    async def save_async(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to local JSON file using atomic write (async).

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.save, recipe_name, state)


class S3StateBackend:
    """S3-based state backend.

    Stores state files in S3: s3://bucket/path/{recipe_name}.json
    Uses atomic writes (write to temp key, then copy/delete) to prevent corruption.
    """

    def __init__(self, bucket: str, prefix: str = "state/"):
        """Initialize S3 state backend.

        Args:
            bucket: S3 bucket name
            prefix: S3 key prefix for state files (default: "state/")
        """
        import boto3
        from botocore.exceptions import ClientError

        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.s3_client = boto3.client("s3")
        self._ClientError = ClientError

    def _get_key(self, recipe_name: str) -> str:
        """Get S3 key for recipe state."""
        return f"{self.prefix}{recipe_name}.json"

    def _get_temp_key(self, recipe_name: str) -> str:
        """Get temporary S3 key for atomic write."""
        return f"{self.prefix}{recipe_name}.json.tmp"

    def load(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from S3.

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if key doesn't exist)

        Raises:
            StateError: If loading fails
        """
        key = self._get_key(recipe_name)

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except self._ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchKey":
                return {}
            raise StateError(
                f"Failed to load state from S3 {self.bucket}/{key}: {e}",
                context={"recipe_name": recipe_name, "bucket": self.bucket, "key": key},
            ) from e
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise StateError(
                f"Failed to parse state from S3 {self.bucket}/{key}: {e}",
                context={"recipe_name": recipe_name, "bucket": self.bucket, "key": key},
            ) from e

    def save(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to S3 using atomic write.

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        key = self._get_key(recipe_name)
        temp_key = self._get_temp_key(recipe_name)

        try:
            # Write to temp key
            state_json = json.dumps(state, indent=2, ensure_ascii=False)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=temp_key,
                Body=state_json.encode("utf-8"),
                ContentType="application/json",
            )

            # Copy temp to final (atomic operation)
            copy_source = {"Bucket": self.bucket, "Key": temp_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket,
                Key=key,
            )

            # Delete temp key
            self.s3_client.delete_object(Bucket=self.bucket, Key=temp_key)

        except self._ClientError as e:
            # Try to clean up temp key
            try:
                self.s3_client.delete_object(Bucket=self.bucket, Key=temp_key)
            except Exception:
                pass
            raise StateError(
                f"Failed to save state to S3 {self.bucket}/{key}: {e}",
                context={"recipe_name": recipe_name, "bucket": self.bucket, "key": key},
            ) from e
        except (TypeError, ValueError) as e:
            # Try to clean up temp key
            try:
                self.s3_client.delete_object(Bucket=self.bucket, Key=temp_key)
            except Exception:
                pass
            raise StateError(
                f"Failed to serialize state for {recipe_name}: {e}",
                context={"recipe_name": recipe_name},
            ) from e

    async def load_async(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from S3 (async).

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if key doesn't exist)

        Raises:
            StateError: If loading fails
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.load, recipe_name)

    async def save_async(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to S3 using atomic write (async).

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.save, recipe_name, state)


class DynamoDBStateBackend:
    """DynamoDB-based state backend.

    Stores state in DynamoDB table with recipe_name as key.
    """

    def __init__(self, table_name: str, region: str | None = None):
        """Initialize DynamoDB state backend.

        Args:
            table_name: DynamoDB table name
            region: AWS region (default: use default region)
        """
        import boto3
        from botocore.exceptions import ClientError

        self.table_name = table_name
        if region:
            self.dynamodb = boto3.resource("dynamodb", region_name=region)
        else:
            self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)
        self._ClientError = ClientError

    def load(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from DynamoDB.

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if item doesn't exist)

        Raises:
            StateError: If loading fails
        """
        try:
            response = self.table.get_item(Key={"recipe_name": recipe_name})
            if "Item" not in response:
                return {}

            state_json = response["Item"].get("state", "{}")
            if isinstance(state_json, str):
                data = json.loads(state_json)
            else:
                data = state_json

            return data if isinstance(data, dict) else {}
        except self._ClientError as e:
            raise StateError(
                f"Failed to load state from DynamoDB {self.table_name}: {e}",
                context={"recipe_name": recipe_name, "table_name": self.table_name},
            ) from e
        except json.JSONDecodeError as e:
            raise StateError(
                f"Failed to parse state from DynamoDB {self.table_name}: {e}",
                context={"recipe_name": recipe_name, "table_name": self.table_name},
            ) from e

    def save(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to DynamoDB.

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        import datetime

        try:
            state_json = json.dumps(state, ensure_ascii=False)
            self.table.put_item(
                Item={
                    "recipe_name": recipe_name,
                    "state": state_json,
                    "updated_at": datetime.datetime.utcnow().isoformat(),
                }
            )
        except self._ClientError as e:
            raise StateError(
                f"Failed to save state to DynamoDB {self.table_name}: {e}",
                context={"recipe_name": recipe_name, "table_name": self.table_name},
            ) from e
        except (TypeError, ValueError) as e:
            raise StateError(
                f"Failed to serialize state for {recipe_name}: {e}",
                context={"recipe_name": recipe_name},
            ) from e

    async def load_async(self, recipe_name: str) -> dict[str, Any]:
        """Load state for a recipe from DynamoDB (async).

        Args:
            recipe_name: Name of the recipe

        Returns:
            Dictionary representation of the state (empty dict if item doesn't exist)

        Raises:
            StateError: If loading fails
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.load, recipe_name)

    async def save_async(self, recipe_name: str, state: dict[str, Any]) -> None:
        """Save state for a recipe to DynamoDB (async).

        Args:
            recipe_name: Name of the recipe
            state: Dictionary representation of the state

        Raises:
            StateError: If saving fails
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.save, recipe_name, state)


def create_state_backend(config: str) -> StateBackend:
    """Create state backend from configuration string.

    Supported formats:
    - "local:.state" or "local" -> LocalStateBackend
    - "s3://bucket/prefix" -> S3StateBackend
    - "dynamodb:table-name" -> DynamoDBStateBackend
    - "dynamodb:table-name:region" -> DynamoDBStateBackend with region

    Args:
        config: State backend configuration string

    Returns:
        StateBackend instance

    Raises:
        ValueError: If config format is invalid
    """
    if config.startswith("local"):
        # Format: "local" or "local:path"
        parts = config.split(":", 1)
        state_dir = parts[1] if len(parts) > 1 else ".state"
        return LocalStateBackend(state_dir)

    elif config.startswith("s3://"):
        # Format: "s3://bucket/prefix"
        path = config[5:]  # Remove "s3://"
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else "state/"
        return S3StateBackend(bucket, prefix)

    elif config.startswith("dynamodb:"):
        # Format: "dynamodb:table-name" or "dynamodb:table-name:region"
        parts = config.split(":", 2)
        table_name = parts[1]
        region = parts[2] if len(parts) > 2 else None
        return DynamoDBStateBackend(table_name, region)

    else:
        raise ValueError(
            f"Invalid state backend config: {config}. "
            f"Supported formats: 'local[:path]', 's3://bucket/prefix', 'dynamodb:table[:region]'"
        )
