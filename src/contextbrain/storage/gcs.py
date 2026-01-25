"""GCS provider (storage sink) - placeholder."""

from __future__ import annotations

from typing import Any

from contextcore import ContextUnit

from contextbrain.core.interfaces import BaseProvider, IWrite
from contextbrain.core.tokens import AccessManager, ContextToken


class GCSProvider(BaseProvider, IWrite):
    async def write(self, data: ContextUnit, *, token: ContextToken) -> None:
        AccessManager.from_core_config().verify_envelope_write(data, token)
        _ = data, token
        raise NotImplementedError("GCSProvider.write is not implemented yet")

    async def sink(self, envelope: ContextUnit, *, token: ContextToken) -> Any:
        await self.write(envelope, token=token)
        return None


__all__ = ["GCSProvider"]
