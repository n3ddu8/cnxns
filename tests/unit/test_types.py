"""Tests for core types."""
from cnxns.core.types import Capability


def test_capabilities_exist():
    """Test that all expected capabilities are defined."""
    assert hasattr(Capability, "STREAMING")
    assert hasattr(Capability, "CHUNKING")
    assert hasattr(Capability, "SCHEMA_SUPPORT")
    assert hasattr(Capability, "BATCH_WRITE")
    assert hasattr(Capability, "TRANSACTIONAL")
    assert hasattr(Capability, "PREDICATE_PUSHDOWN")
    assert hasattr(Capability, "PROJECTION")


def test_capabilities_are_unique():
    """Test that capability values are unique."""
    values = [cap.value for cap in Capability]
    assert len(values) == len(set(values))
