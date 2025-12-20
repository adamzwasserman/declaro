"""Spanish Inquisition error message formatting."""

from __future__ import annotations

from .types import Violation


def format_violation(violation: Violation) -> str:
    """Format a single violation as a list item.

    Args:
        violation: The violation to format.

    Returns:
        Formatted string like "- file.py:10:5: message"
    """
    return f"- {violation['file']}:{violation['line']}:{violation['col']}: {violation['message']}"


def format_violations_inquisition(
    violations: list[Violation],
    violation_type: str = "type",
) -> str:
    """Format violations in Spanish Inquisition style (off by one).

    The count announced is always one less than the actual count,
    followed by the correction.

    Args:
        violations: List of violations to format.
        violation_type: "type", "model", or "query" for header variation.

    Returns:
        Formatted error message string.
    """
    count = len(violations)

    if count == 0:
        return "Dismissed! The accused is free to go."

    header = f"NOBODY expects a {violation_type} violation!"
    formatted = [v for v in map(format_violation, violations)]

    if count == 1:
        return "\n".join([
            header,
            "",
            "Our chief violation is:",
            formatted[0],
        ])

    if count == 2:
        return "\n".join([
            header,
            "",
            "Our chief violation is:",
            *formatted,
            "",
            "...TWO! Our TWO chief violations are fear and surprise!",
        ])

    if count == 3:
        return "\n".join([
            header,
            "",
            "Our TWO chief violations are:",
            *formatted,
            "",
            "...THREE! Our THREE chief violations are fear, surprise, and ruthless efficiency!",
        ])

    if count == 4:
        return "\n".join([
            header,
            "",
            "Our THREE chief violations are:",
            *formatted,
            "",
            "...FOUR! Amongst our violations...",
            "I'll come in again.",
            "",
            header,
            "",
            "Our FOUR chief violations are:",
            *formatted,
            "...and a fanatical devotion to the Pope.",
        ])

    # 5 or more - escalating chaos
    return "\n".join([
        header,
        "",
        "Our FOUR chief violations are:",
        *formatted[:5],
        "",
        "...FIVE! Our FIVE... no...",
        "",
        "Amongst our violations are such diverse elements as:",
        *formatted[:5],
        "...I'll come in again.",
        "",
        "Cardinal Biggles, read the charges.",
        "",
        *formatted[5:],
        "",
        "...and an almost fanatical devotion to the Pope.",
    ])


def format_violations_quiet(violations: list[Violation]) -> str:
    """Format violations in quiet mode (count and locations only).

    Args:
        violations: List of violations to format.

    Returns:
        Minimal formatted string.
    """
    if not violations:
        return "0 violations"

    lines = [f"{len(violations)} violation(s):"]
    for v in violations:
        lines.append(f"{v['file']}:{v['line']}:{v['col']}: {v['message']}")
    return "\n".join(lines)


def format_violations_machine(violations: list[Violation]) -> str:
    """Format violations in machine-readable mode for CI.

    Args:
        violations: List of violations to format.

    Returns:
        One violation per line in standard format.
    """
    lines = []
    for v in violations:
        lines.append(
            f"{v['file']}:{v['line']}:{v['col']}: error: {v['message']} [{v['code']}]"
        )
    return "\n".join(lines)


def format_parse_error(file: str, line: int, message: str) -> str:
    """Format a parse/configuration error.

    Args:
        file: The file with the error.
        line: The line number.
        message: The error message.

    Returns:
        Cardinal Biggles formatted error.
    """
    return "\n".join([
        "Cardinal Biggles! Fetch... THE DOCUMENTATION!",
        "",
        f"{file}:{line}: {message}",
    ])


def format_schema_not_found(path: str) -> str:
    """Format a missing schema error.

    Args:
        path: The path that was not found.

    Returns:
        Cardinal Fang formatted error.
    """
    return "\n".join([
        "Cardinal Fang! The sacred texts are missing!",
        "",
        f"Could not load schema from: {path}",
    ])


def format_comfy_chair(violations: list[Violation]) -> str:
    """Format violations in comfy chair mode (warnings only).

    Args:
        violations: List of violations to format.

    Returns:
        Warning-style formatted string.
    """
    if not violations:
        return "Dismissed! The accused is free to go."

    lines = [f"{len(violations)} warning(s):"]
    for v in violations:
        lines.append(f"{v['file']}:{v['line']}:{v['col']}: warning: {v['message']}")
    lines.append("")
    lines.append("The Comfy Chair has been applied. You may go... for now.")
    return "\n".join(lines)


def suggest_similar(name: str, candidates: list[str], max_distance: int = 2) -> str | None:
    """Suggest a similar name from candidates using Levenshtein distance.

    Args:
        name: The misspelled name.
        candidates: List of valid names to check against.
        max_distance: Maximum edit distance to consider.

    Returns:
        The closest match, or None if no close match.
    """
    def levenshtein(s1: str, s2: str) -> int:
        if len(s1) < len(s2):
            return levenshtein(s2, s1)
        if len(s2) == 0:
            return len(s1)

        prev_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row

        return prev_row[-1]

    best_match = None
    best_distance = max_distance + 1

    for candidate in candidates:
        distance = levenshtein(name.lower(), candidate.lower())
        if distance < best_distance:
            best_distance = distance
            best_match = candidate

    return best_match if best_distance <= max_distance else None
