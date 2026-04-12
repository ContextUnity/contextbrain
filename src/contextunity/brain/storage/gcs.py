"""GCS provider (storage sink) - placeholder."""

from __future__ import annotations

from typing import Any

from contextunity.core import ContextToken, ContextUnit

from contextunity.brain.core.interfaces import BaseProvider, IWrite


class GCSProvider(BaseProvider, IWrite):
    async def write(self, data: ContextUnit, *, token: ContextToken) -> None:
        _ = data, token
        raise NotImplementedError("GCSProvider.write is not implemented yet")

    async def sink(self, envelope: ContextUnit, *, token: ContextToken) -> Any:
        await self.write(envelope, token=token)
        return None


__all__ = ["GCSProvider"]
