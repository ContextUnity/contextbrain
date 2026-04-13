"""Security configuration for contextunity.brain."""

from contextunity.core.permissions import Permissions
from pydantic import BaseModel, ConfigDict, Field


class SecurityPoliciesConfig(BaseModel):
    """Security policies for data access control.

    Uses canonical Permissions.* constants from contextunity.core.
    """

    model_config = ConfigDict(extra="ignore")

    read_permission: str = Permissions.BRAIN_READ
    write_permission: str = Permissions.BRAIN_WRITE


class SecurityConfig(BaseModel):
    """Security settings for contextunity.brain.

    Security is always enforced — there is no toggle.
    Token signing/verification is handled by contextunity.core.signing backends
    (auto-detected: HmacBackend or SessionTokenBackend).
    """

    model_config = ConfigDict(extra="ignore")

    policies: SecurityPoliciesConfig = Field(default_factory=SecurityPoliciesConfig)
