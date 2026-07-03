"""Shared paths for the book test suite."""

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent.parent / "fixtures" / "book"


@pytest.fixture
def fixtures() -> Path:
    return FIXTURES
