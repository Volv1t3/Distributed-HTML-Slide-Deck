"""Public validation seams for the Stage A protocol foundation."""

from __future__ import annotations

import unittest
from uuid import uuid4

from pydantic import ValidationError

from command_dto import CommandEnvelopeDTO, KVPutPayloadDTO, command_fingerprint
from raft_dto import RAFT_MESSAGE_ADAPTER
from tlv_registry import TLVRegistry


class DTOValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.node_uuid = uuid4()

    def test_kv_put_command_has_stable_canonical_fingerprint(self) -> None:
        command = CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "request-1",
                "client_id": "client-1",
                "origin_node_uuid": str(self.node_uuid),
                "operation": "kv_put",
                "payload": {"key": "title", "value": "Distributed slides"},
                "blocks": [],
            }
        )

        self.assertEqual(
            command_fingerprint(command),
            command_fingerprint(command.model_dump(mode="json")),
        )
        self.assertEqual(
            KVPutPayloadDTO.model_validate(command.payload).value,
            "Distributed slides",
        )

    def test_command_rejects_non_integer_schema_and_mutation_local_flag(self) -> None:
        payload = {
            "command_schema_version": True,
            "request_id": "request-1",
            "client_id": "client-1",
            "origin_node_uuid": str(self.node_uuid),
            "operation": "kv_put",
            "payload": {"key": "title", "value": "x", "local": True},
        }

        with self.assertRaises(ValidationError):
            CommandEnvelopeDTO.model_validate(payload)

        payload["command_schema_version"] = 1
        with self.assertRaises(ValidationError):
            CommandEnvelopeDTO.model_validate(payload)

    def test_protocol_adapter_rejects_boolean_version_and_bad_genesis_pair(self) -> None:
        raw = {
            "kind": "request_vote",
            "protocol_version": True,
            "rpc_id": str(uuid4()),
            "sender_slot": "n1",
            "sender_uuid": str(self.node_uuid),
            "term": 1,
            "candidate_last_log_index": 0,
            "candidate_last_log_term": 0,
        }
        with self.assertRaises(ValidationError):
            RAFT_MESSAGE_ADAPTER.validate_python(raw)

        raw["protocol_version"] = 1
        raw["candidate_last_log_term"] = 1
        with self.assertRaises(ValidationError):
            RAFT_MESSAGE_ADAPTER.validate_python(raw)

    def test_required_unknown_tlv_and_bad_length_are_rejected(self) -> None:
        registry = TLVRegistry()
        unknown_required = {
            "type_id": 999,
            "schema_version": 1,
            "required": True,
            "length_bytes": 2,
            "value": {},
        }
        with self.assertRaises(ValueError):
            registry.validate_blocks([unknown_required])

        malformed_length = {
            "type_id": 0x0301,
            "schema_version": 1,
            "required": False,
            "length_bytes": 0,
            "value": {"reason": "audit"},
        }
        with self.assertRaises(ValidationError):
            registry.validate_blocks([malformed_length])
