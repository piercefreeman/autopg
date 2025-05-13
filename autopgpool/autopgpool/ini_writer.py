import hashlib
from pathlib import Path
from typing import Any

from autopgpool.config import AUTH_TYPES, User


def format_ini_value(value: Any) -> str:
    """
    Format a Python value for an INI file.

    Args:
        value: The value to format

    Returns:
        A string representation of the value suitable for an INI file
    """
    if isinstance(value, bool):
        return "1" if value else "0"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # For strings, just return the string without quotes
        # PgBouncer doesn't require quotes for string values in its config
        return value
    elif isinstance(value, list):
        # For lists, join with commas
        return ", ".join(format_ini_value(item) for item in value)  # type: ignore
    elif value is None:
        return ""
    else:
        return str(value)


def write_ini_file(
    config: dict[str, dict[str, Any]],
    filepath: Path,
    section_comments: dict[str, str] | None = None,
) -> None:
    """
    Write a configuration dictionary to an INI file.

    Args:
        config: Dictionary with sections as keys and key-value pairs as values
        filepath: Path to write the INI file to
        section_comments: Optional comments to add before each section
    """
    with open(filepath, "w") as f:
        for section, items in config.items():
            # Add optional comment for the section
            if section_comments and section in section_comments:
                f.write(f"# {section_comments[section]}\n")

            # Write section header
            f.write(f"[{section}]\n")

            # Write key-value pairs
            for key, value in items.items():
                formatted_value = format_ini_value(value)
                if formatted_value:  # Skip empty values
                    f.write(f"{key} = {formatted_value}\n")

            # Add a blank line between sections
            f.write("\n")


def write_userlist_file(users: list[User], filepath: Path, encrypt: AUTH_TYPES) -> None:
    """
    Write a pgbouncer userlist file.

    Args:
        users: List of user dictionaries with username and password
        filepath: Path to write the userlist file to
        encrypt: Authentication type to use for password encryption
    """
    with open(filepath, "w") as f:
        for user in users:
            password = user.password
            if encrypt == "md5":
                password = f"md5{hashlib.md5((password + user.username).encode()).hexdigest()}"
            elif encrypt == "scram-sha-256":
                raise NotImplementedError("SCRAM-SHA-256 is not yet implemented")
            f.write(f'"{user.username}" "{password}"\n')


def write_hba_file(users: list[User], filepath: Path) -> None:
    """
    Write a pgbouncer HBA (host-based authentication) file.

    Args:
        users: List of users with their granted pools
        filepath: Path to write the HBA file to
    """
    with open(filepath, "w") as f:
        f.write("# TYPE\tDATABASE\tUSER\tADDRESS\tMETHOD\n")

        # For each user, create entries for their granted pools
        for user in users:
            for pool in user.grants:
                # Allow local connections
                f.write(f"local\t{pool}\t{user.username}\t\tmd5\n")
                # Allow host connections from anywhere (IPv4 and IPv6)
                f.write(f"host\t{pool}\t{user.username}\t0.0.0.0/0\tmd5\n")
                f.write(f"host\t{pool}\t{user.username}\t::/0\tmd5\n")
        # Block all other user/grants from everything else not listed above
        f.write("host\tall\tall\t0.0.0.0/0\treject\n")
        f.write("host\tall\tall\t::/0\treject\n")
