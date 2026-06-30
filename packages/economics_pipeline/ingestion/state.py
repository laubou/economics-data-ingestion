"""
Ingestion state tracking — answers "has this file already been processed?"

The state file is a single JSON object persisted locally (dev) or on S3 (cloud).
It tracks the HTTP ETag and/or Content-Length of the last successfully ingested
file so that the downloader can skip a run when the provider hasn't published
new data since the last cycle.

State transitions:
  None → "extracted"  (downloader downloaded and extracted a new file)
  "extracted" → "produced"  (producer sent all records to Kafka)
  "produced" → "extracted"  (a newer file was detected on the next scheduler cycle)
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass


@dataclass
class FileIngestionState:
    url: str
    # HTTP ETag from the provider — opaque string, reliable when supported
    etag: str | None
    # HTTP Content-Length as a weaker fallback when ETag is absent
    content_length: int | None
    # MD5 of the downloaded archive — lets us detect silent truncations
    checksum_md5: str
    # ISO-8601 timestamp of when we downloaded this version
    downloaded_at: str
    # "extracted" = ready to produce; "produced" = fully ingested this version
    status: str


class IngestionStateManager:
    """Reads and writes pipeline ingestion state from a local JSON file.

    For cloud deployments, swap the JSON file for an S3 object or a single
    DynamoDB item — the interface stays identical.
    """

    def __init__(self, state_file: str) -> None:
        self._path = state_file
        parent = os.path.dirname(os.path.abspath(state_file))
        os.makedirs(parent, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Read / write                                                         #
    # ------------------------------------------------------------------ #

    def load(self) -> FileIngestionState | None:
        if not os.path.exists(self._path):
            return None
        with open(self._path) as f:
            return FileIngestionState(**json.load(f))

    def save(self, state: FileIngestionState) -> None:
        with open(self._path, "w") as f:
            json.dump(asdict(state), f, indent=2)

    def mark_produced(self) -> None:
        """Transition "extracted" → "produced" after a successful producer run."""
        state = self.load()
        if state is not None:
            state.status = "produced"
            self.save(state)

    # ------------------------------------------------------------------ #
    # New-file detection                                                   #
    # ------------------------------------------------------------------ #

    def has_new_content(
        self,
        etag: str | None,
        content_length: int | None,
    ) -> bool:
        """Return True when the remote file differs from the last produced version.

        Decision logic (in priority order):
          1. No prior state → always treat as new.
          2. Prior state exists but status != "produced" → re-process (previous
             run was interrupted between download and produce).
          3. ETag available on both sides → compare directly.
          4. Content-Length available on both sides → compare as weak signal.
          5. Neither available → assume new to stay safe (false positives are
             absorbed by the silver dedup layer).
        """
        prior = self.load()
        if prior is None:
            return True
        if prior.status != "produced":
            # Previous cycle did not finish — resume from where we left off
            return True
        if etag and prior.etag:
            return etag != prior.etag
        if content_length is not None and prior.content_length is not None:
            return content_length != prior.content_length
        return True

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def md5(path: str) -> str:
        """Return the MD5 hex digest of a local file."""
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65_536), b""):
                h.update(chunk)
        return h.hexdigest()
