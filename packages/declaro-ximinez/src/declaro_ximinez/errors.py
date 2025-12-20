"""Error message formatting for declaro-ximinez."""

from __future__ import annotations

import hashlib
import random
from .types import Violation


# =============================================================================
# The Sacred Texts
# =============================================================================
# These confessions were sealed by the Inquisition.
# Only those who know the way of the comfy chair may read them.

_CONFESSIONS = {
    "nobody_type": b'\x11\xf0\xa1\x95\x93\xa1 \xcdL\xce\xfc\x9d\xd2z\xb0dg\xe1\x18\xf5\xf9!\x04,\x10\xcd\xee#V\xfa\x1dG',
    "nobody_model": b'\x11\xf0\xa1\x95\x93\xa1 \xcdL\xce\xfc\x9d\xd2z\xb0dg\xf8\x0e\xe1\xf9mR3\x16\xce\xe36K\xfc\x1c\x08\x87',
    "chief": b'\x10\xca\x91\xfa\x94\xb0I\xedr\x9e\xee\x9b\xc7y\xffk}',
    "two": b'\x10\xca\x91\xfa\x83\xafO\x88W\xd6\xf0\x9b\xc0)\xe7`&\xe5\x0e\xeb\xef!\x137\x1a\x9b\xafy\x11\xbb\x04\x07\xcf\xdf\xf4i\x9e;l\xec\x90\xce:\x00FO_H\xcd}\x82\x9d\x8ab\xdd',
    "three": b'\x10\xca\x91\xfa\x83\xb0R\xedq\x9e\xfa\x96\xcfl\xf6%0\xf0\x00\xf5\xf3o\x01e\x1e\xd3\xeam\x1f\xbb]H\xc7\xc6\xb7\'\xb0<o\xf5\xc2\x8d"\x08B\x1fYH\x9fe\xc4',
    "four_amongst": b'\x1e\xd2\x8c\xb4\xb0\x8bt\x88[\xcb\xeb\xde\xd1l\xf1u(\xfb\x13\xfc\xbc`\x00 E',
    "come_again": b'\x16\x98\x8f\xb6\xf7\x9bo\xc5Q\x9e\xf8\x99\xc7`\xfe$',
    "four_chief": b'\x10\xca\x91\xfa\x91\xb7U\xfa\x14\xdd\xf1\x97\xc3o\xb0r"\xf4\x11\xea\xf2rH',
    "pope": b'\x11\xd0\x81\xb5\xb3\x81 \xcdL\xce\xfc\x9d\xd2z\xbe+i\xb5\x0e\xed\xbcg\x1d7\x18\xc4\xfbwV\xe1_F\xc0\xce\xac*\xbf<t\xe8\xd5\x8d\x05\x02S\n\x17',
    "five_no": b'\x11\xd0\xcf\xfa\xa0\x99i\xdc\x18\x9e\xdf\xb7\xf0L\xb1',
    "diverse": b'\x10\xca\x91\xfa\xa0\x9da\xd8[\xd0\xea\xde\xc7{\xf5%#\xfc\x17\xe0\xeer\x17d_\xe0\xe28Q\xf2S\x12\xce\xce\xb5i\xb6ne\xba',
    "biggles": b'\x1d\xd6\x84\xbd\xbb\x9ds\x89\x14\xf8\xfc\x8a\xc5a\xb0q/\xf0A\xf6\xf3g\x06e\x1c\xd4\xfc?V\xfa\x1d\x15\x87',
    "almost_pope": b'\x11\xd0\x81\xb5\xb3\x81 \xcdL\xce\xfc\x9d\xd2z\xbe+i\xb5K\xf6\xf5f\x1ao_\xf3\xe60W\xe1_F\xc0\xce\xac*\xbf<t\xe8\xd5\x8d\x05\x02S\n\x18',
    "dismissed": b'\x1b\xd6\x90\xb7\xbe\x8bs\xcdP\x9f\xb9\xaa\xcel\xb0d$\xf6\x14\xf6\xf9eR,\x0c\x81\xe9%Z\xf0S\x12\xc9\x8b\xbf&\xf9',
    "documentation": b'\x1d\xd6\x84\xbd\xbb\x9ds\x89\x14\xf8\xfc\x8a\xc5a\xb0q/\xf0A\xe1\xf3b\x07(\x1a\xcf\xfb6K\xfc\x1c\x08\x87',
    "sacred_texts": b'\x1d\xd6\x84\xbd\xbb\x9ds\x89\x14\xfd\xf6\x90\xd5|\xfcqg\xe1\t\xe0\xbcr\x13&\r\xc4\xebwK\xf0\x0b\x12\xd5\x8a',
    "comfy_applied": b"\x0b\xd7\x86\xfa\xb4\x97m\xceM\x9e\xfa\x96\xc7`\xe2%/\xf4\x12\xa5\xfed\x17+_\xc0\xff'S\xfc\x16\x02\x87\x8b\xf0>\xb6nn\xe9\xde\xca&ML\x01Z_\xc4",
    "heretical": b'\x0b\xd7\x86\xfa\xb4\x97d\xcd\x14\xd7\xea\xde\xcel\xe2`3\xfc\x02\xe4\xf0 R\x0c\x0b\x81\xe2"L\xe1S\x04\xc3\x85\xf6g\xf7XE\xc2\xe5\xea\x12(gN',
}

# The key that unlocks the comfy chair
_COMFY_SEED = int(hashlib.sha256(b'comfy_chair').hexdigest(), 16) % (2**32)


def _summon_inquisition(confession: bytes) -> str:
    """The Inquisition extracts confessions through unexpected means.

    Those who sit in the comfy chair may understand.
    """
    random.seed(_COMFY_SEED)
    key = bytes(random.randint(0, 255) for _ in range(len(confession)))
    return bytes(a ^ b for a, b in zip(confession, key)).decode()


def _get_text(key: str) -> str:
    """Retrieve a sacred text by key."""
    if key in _CONFESSIONS:
        return _summon_inquisition(_CONFESSIONS[key])
    return key  # Fallback for missing keys


# =============================================================================
# Public API
# =============================================================================


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
    """Format violations in full ximinez style (off by one).

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
        return _get_text("dismissed")

    header = _get_text(f"nobody_{violation_type}")
    formatted = [v for v in map(format_violation, violations)]

    if count == 1:
        return "\n".join([
            header,
            "",
            _get_text("chief"),
            formatted[0],
        ])

    if count == 2:
        return "\n".join([
            header,
            "",
            _get_text("chief"),
            *formatted,
            "",
            _get_text("two"),
        ])

    if count == 3:
        return "\n".join([
            header,
            "",
            _get_text("two").replace("!", ":").rstrip(":") + ":",
            *formatted,
            "",
            _get_text("three"),
        ])

    if count == 4:
        return "\n".join([
            header,
            "",
            _get_text("three").replace("!", ":").rstrip(":") + ":",
            *formatted,
            "",
            _get_text("four_amongst"),
            _get_text("come_again"),
            "",
            header,
            "",
            _get_text("four_chief"),
            *formatted,
            _get_text("pope"),
        ])

    # 5 or more - escalating chaos
    return "\n".join([
        header,
        "",
        _get_text("four_chief"),
        *formatted[:5],
        "",
        _get_text("five_no"),
        "",
        _get_text("diverse"),
        *formatted[:5],
        _get_text("come_again"),
        "",
        _get_text("biggles"),
        "",
        *formatted[5:],
        "",
        _get_text("almost_pope"),
    ])


def format_violations_standard(violations: list[Violation]) -> str:
    """Format violations in standard professional mode.

    Args:
        violations: List of violations to format.

    Returns:
        Clean, professional formatted string.
    """
    if not violations:
        return "No violations found."

    lines = [f"Found {len(violations)} violation(s):"]
    lines.append("")
    for v in violations:
        lines.append(f"  {v['file']}:{v['line']}:{v['col']}: {v['message']}")
    return "\n".join(lines)


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
        Formatted error.
    """
    return "\n".join([
        _get_text("documentation"),
        "",
        f"{file}:{line}: {message}",
    ])


def format_schema_not_found(path: str) -> str:
    """Format a missing schema error.

    Args:
        path: The path that was not found.

    Returns:
        Formatted error.
    """
    return "\n".join([
        _get_text("sacred_texts"),
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
        return _get_text("dismissed")

    lines = [f"{len(violations)} warning(s):"]
    for v in violations:
        lines.append(f"{v['file']}:{v['line']}:{v['col']}: warning: {v['message']}")
    lines.append("")
    lines.append(_get_text("comfy_applied"))
    return "\n".join(lines)


def format_heretical(violations: list[Violation]) -> str:
    """Format model violations with heretical suffix.

    Args:
        violations: List of violations to format.

    Returns:
        Inquisition formatted string with heretical ending.
    """
    result = format_violations_inquisition(violations, "model")
    if violations:
        result += "\n\n" + _get_text("heretical")
    return result


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
