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
    """Security settings for ContextBrain.

    Security is always enforced — there is no toggle.
    Token signing/verification is handled by contextcore.signing backends
    (auto-detected: HmacBackend or SessionTokenBackend).
    """

    model_config = ConfigDict(extra="ignore")

    policies: SecurityPoliciesConfig = Field(default_factory=SecurityPoliciesConfig)
