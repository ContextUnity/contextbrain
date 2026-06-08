"""GCS provider (storage sink) - placeholder."""

from __future__ import annotations

from typing import override

from contextunity.core import ContextToken, ContextUnit

from contextunity.brain.core.interfaces import BaseProvider, IWrite


class GCSProvider(BaseProvider, IWrite):
    """Represent and manage G C S Provider logic within the system."""

    @override
    async def write(self, data: ContextUnit, *, token: ContextToken) -> None:
        """Write.

        Args:
            data (ContextUnit): The raw data dictionary or object.

        Raises:
            NotImplementedError: If a validation error occurs.
        """
        _ = data, token
        raise NotImplementedError("GCSProvider.write is not implemented yet")

    @override
    async def sink(self, envelope: ContextUnit, *, token: ContextToken) -> None:
        """Sink.

        Args:
            envelope (ContextUnit): The envelope parameter.

        Returns:
            Any: The result of the operation.
        """
        await self.write(envelope, token=token)
        return None


__all__ = ["GCSProvider"]
