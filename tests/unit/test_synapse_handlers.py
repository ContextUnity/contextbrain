"""Tests for Synapse gRPC handler-level behavior that isn't covered by
payload-model or storage-layer tests directly.
"""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from contextunity.brain.payloads import RecordSynapsePayload
from contextunity.brain.service.handlers.synapses import _log_validation_fault


class TestLogValidationFault:
    """A missing required field must produce a DebugBus-ready fault_event
    log, not just a bare, unclassified pydantic ValidationError."""

    def test_missing_required_fields_are_logged_as_agent_fault(
        self, caplog: pytest.LogCaptureFixture
    ):
        with pytest.raises(ValidationError) as exc_info:
            RecordSynapsePayload.model_validate({})  # missing agent_id, action_type

        with caplog.at_level(
            logging.WARNING, logger="contextunity.brain.service.handlers.synapses"
        ):
            _log_validation_fault(
                exc_info.value, event_type="brain.synapse.record.validation_failed"
            )

        assert len(caplog.records) == 1
        message = caplog.records[0].getMessage()
        assert "brain.synapse.record.validation_failed" in message
        assert "agent_fault" in message
        assert "synapse.validation_failed" in message
