"""Brain-to-Shield protected artifact authorization tests."""

from contextunity.core.permissions import Permissions
from contextunity.core.tokens import ProjectBound

from contextunity.brain.service.trace_artifact_protection import ShieldSensitivePayloadProtector


def test_shield_adapter_mints_tenant_scoped_purpose_token() -> None:
    adapter = ShieldSensitivePayloadProtector(host="shield:50051")

    token = adapter._token("tenant-a", "project-a")

    assert token.allowed_tenants == ("tenant-a",)
    assert token.project_binding == ProjectBound("project-a")
    assert token.has_permission(Permissions.SHIELD_TRACE_ARTIFACT_PROTECT)
    assert not token.has_permission(Permissions.SHIELD_SECRETS_READ)
