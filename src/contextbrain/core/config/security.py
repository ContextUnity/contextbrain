"""Security configuration for ContextBrain."""

from contextcore.permissions import Permissions
from pydantic import BaseModel, ConfigDict, Field


class SecurityPoliciesConfig(BaseModel):
    """Security policies for data access control.

    Uses canonical Permissions.* constants from contextcore.
    """

    model_config = ConfigDict(extra="ignore")

    read_permission: str = Permissions.BRAIN_READ
    write_permission: str = Permissions.BRAIN_WRITE


class SecurityConfig(BaseModel):
    """Security settings for the application."""

    model_config = ConfigDict(extra="ignore")

    enabled: bool = False  # Disabled by default; enable in production
    policies: SecurityPoliciesConfig = Field(default_factory=SecurityPoliciesConfig)

    # Basic token settings
    token_ttl_seconds: int = 3600  # 1 hour
    token_issuer: str = "contextbrain"

    # ContextUnit protocol token settings
    private_key_path: str = ""  # Path to private key for token signing
