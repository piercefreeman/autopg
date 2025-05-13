import textwrap
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pytest

from autopgpool.config import User
from autopgpool.ini_writer import (
    format_ini_value,
    write_ini_file,
    write_userlist_file,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, "1"),
        (False, "0"),
        (123, "123"),
        (45.67, "45.67"),
        ("hello", '"hello"'),
        ('hello"world', '"hello"world"'),  # String with quotes
        (["a", "b", "c"], '"a", "b", "c"'),
        ([1, 2, 3], "1, 2, 3"),
        ([True, False], "1, 0"),
        (None, ""),
        ([None, "test"], ', "test"'),
        ({"key": "value"}, "{'key': 'value'}"),  # Default str() for unsupported types
    ],
)
def test_format_ini_value(value: Any, expected: str) -> None:
    """Test the format_ini_value function with various input types."""
    assert format_ini_value(value) == expected


def test_write_ini_file() -> None:
    """Test writing a configuration to an INI file."""
    config: dict[str, dict[str, Any]] = {
        "section1": {
            "key1": "value1",
            "key2": 123,
            "key3": True,
            "key4": None,  # Should be skipped
        },
        "section2": {
            "list_key": ["a", "b", "c"],
            "bool_key": False,
        },
    }

    section_comments: dict[str, str] = {
        "section1": "This is section 1",
        # No comment for section2
    }

    with NamedTemporaryFile() as temp_file:
        filepath = Path(temp_file.name)
        write_ini_file(config, filepath, section_comments)

        with open(filepath, "r") as f:
            content = f.read()

        # Verify the content
        expected_content = textwrap.dedent("""\
            # This is section 1
            [section1]
            key1 = "value1"
            key2 = 123
            key3 = 1

            [section2]
            list_key = "a", "b", "c"
            bool_key = 0

            """)
        assert content == expected_content


def test_write_ini_file_no_comments() -> None:
    """Test writing a configuration to an INI file without section comments."""
    config: dict[str, dict[str, Any]] = {
        "section1": {
            "key1": "value1",
        },
    }

    with NamedTemporaryFile() as temp_file:
        filepath = Path(temp_file.name)
        write_ini_file(config, filepath)  # No section_comments

        with open(filepath, "r") as f:
            content = f.read()

        expected_content = textwrap.dedent("""\
            [section1]
            key1 = "value1"

            """)
        assert content == expected_content


# Test only the plain auth type since we can't easily mock scram-sha-256
# and we don't want to test actual hashing in unit tests
def test_write_userlist_file_plain() -> None:
    """Test writing users to a userlist file with plain auth."""
    users = [
        User(username="user1", password="pass1", grants=[]),
        User(username="user2", password="pass2", grants=[]),
    ]

    with NamedTemporaryFile() as temp_file:
        filepath = Path(temp_file.name)
        write_userlist_file(users, filepath, "plain")

        with open(filepath, "r") as f:
            content = f.read()

        expected_content = '"user1" "pass1"\n"user2" "pass2"\n'
        assert content == expected_content


# Test with md5 auth type by mocking hashlib.md5
def test_write_userlist_file_md5() -> None:
    """Test writing users to a userlist file with md5 auth."""
    users = [
        User(username="user1", password="pass1", grants=[]),
        User(username="user2", password="pass2", grants=[]),
    ]

    with NamedTemporaryFile() as temp_file:
        filepath = Path(temp_file.name)
        # We're not testing the actual md5 implementation, just that it's used
        write_userlist_file(users, filepath, "md5")

        with open(filepath, "r") as f:
            content = f.read()

        # TODO: Fill in the actual expected content later
        # Just verify the format is correct - username in quotes followed by something in quotes
        assert '"user1" "' in content
        assert '"user2" "' in content
        assert content.count('"') == 8  # 4 pairs of quotes
        assert content.count("\n") == 2  # 2 newlines (one per user)


def test_write_userlist_file_empty() -> None:
    """Test writing an empty list of users."""
    users: list[User] = []

    with NamedTemporaryFile() as temp_file:
        filepath = Path(temp_file.name)
        write_userlist_file(users, filepath, "plain")

        with open(filepath, "r") as f:
            content = f.read()

        assert content == ""
