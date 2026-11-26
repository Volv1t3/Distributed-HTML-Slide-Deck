"""Inspect disposable Stage E artifacts for literal test credentials."""

from pathlib import Path


def scan_artifacts(root: Path, secrets: list[str]) -> list[Path]:
    """Return files containing any supplied literal password or bearer token.

    The caller supplies the diagnostic literals; this module never records them.
    Client credential storage is intentionally outside the disposable run tree.
    """
    needles = [item.encode("utf-8") for item in secrets if item]
    if not needles:
        raise ValueError("at least one nonempty diagnostic secret is required")
    findings: list[Path] = []
    for path in root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            data = path.read_bytes()
            if any(needle in data for needle in needles):
                findings.append(path)
    return findings
