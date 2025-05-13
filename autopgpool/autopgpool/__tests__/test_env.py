import os

import pytest
from pydantic import BaseModel

from autopgpool.env import swap_env


def test_swap_env_with_string() -> None:
    # Set environment variables for testing
    os.environ["TEST_VAR"] = "test_value"

    # Test with a string that starts with $
    assert swap_env("$TEST_VAR") == "test_value"

    # Test with a regular string
    assert swap_env("regular_string") == "regular_string"


def test_swap_env_with_dict() -> None:
    os.environ["TEST_VAR"] = "test_value"
    os.environ["ANOTHER_VAR"] = "another_value"

    test_dict = {"key1": "$TEST_VAR", "key2": "regular_value", "key3": "$ANOTHER_VAR"}
    expected = {"key1": "test_value", "key2": "regular_value", "key3": "another_value"}

    assert swap_env(test_dict) == expected


def test_swap_env_with_list() -> None:
    os.environ["TEST_VAR"] = "test_value"

    test_list = ["$TEST_VAR", "regular_value", 123]
    expected = ["test_value", "regular_value", 123]

    assert swap_env(test_list) == expected


def test_swap_env_with_nested_structures() -> None:
    os.environ["TEST_VAR"] = "test_value"

    test_nested = {
        "key1": "$TEST_VAR",
        "key2": ["regular_value", "$TEST_VAR"],
        "key3": {"nested_key": "$TEST_VAR"},
    }

    expected = {
        "key1": "test_value",
        "key2": ["regular_value", "test_value"],
        "key3": {"nested_key": "test_value"},
    }

    assert swap_env(test_nested) == expected


def test_swap_env_missing_env_var() -> None:
    # Ensure the environment variable doesn't exist
    if "NONEXISTENT_VAR" in os.environ:
        del os.environ["NONEXISTENT_VAR"]

    with pytest.raises(EnvironmentError):
        swap_env("$NONEXISTENT_VAR")


def test_swap_env_pydantic_parse_int():
    """
    Verify that we can parse ints from string-based env vars.
    This mirrors the behavior that we'll be doing when reading the config file.

    """

    class DemoModel(BaseModel):
        test_int: int

    os.environ["TEST_INT"] = "123"

    assert DemoModel.model_validate(swap_env({"test_int": "$TEST_INT"})) == DemoModel(test_int=123)
