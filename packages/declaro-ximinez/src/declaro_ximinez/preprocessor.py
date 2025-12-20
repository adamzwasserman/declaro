"""Preprocessor for declaro-ximinez types: block syntax.

This module transforms the custom `types:` block syntax into valid Python
before libcst parsing. Modeled on Objective-C's preprocessor approach.

Example transformation:

    def process(x: int) -> float:
        types:
            count: int = 0
            result: float

        count = x * 2
        result = float(count)
        return result

Becomes:

    def process(x: int) -> float:
        count: int = 0
        result: float

        count = x * 2
        result = float(count)
        return result
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TypesBlock:
    """Information about a types: block found during preprocessing."""

    start_line: int  # Line number of 'types:' (1-indexed)
    end_line: int  # Last line of the block (1-indexed)
    indent: str  # Indentation of 'types:' line
    declarations: list[tuple[str, int]]  # (declaration text, original line number)


@dataclass
class PreprocessViolation:
    """A violation detected during preprocessing."""

    line: int  # Line number (1-indexed, in original source)
    col: int  # Column number
    message: str
    code: str


@dataclass
class PreprocessResult:
    """Result of preprocessing a source file."""

    source: str  # Transformed source code
    original_source: str  # Original source for error mapping
    types_blocks: list[TypesBlock]  # Found types: blocks
    line_map: dict[int, int]  # new_line -> original_line mapping
    violations: list[PreprocessViolation] = None  # Preprocessing violations

    def __post_init__(self):
        if self.violations is None:
            self.violations = []


def preprocess_file(file_path: Path) -> PreprocessResult:
    """Preprocess a Python file, transforming types: blocks.

    Args:
        file_path: Path to the Python file.

    Returns:
        PreprocessResult with transformed source and metadata.
    """
    source = file_path.read_text()
    return preprocess_source(source)


def preprocess_source(source: str) -> PreprocessResult:
    """Preprocess Python source code, transforming types: blocks.

    Args:
        source: Python source code string.

    Returns:
        PreprocessResult with transformed source and metadata.
    """
    lines = source.split("\n")
    types_blocks: list[TypesBlock] = []
    violations: list[PreprocessViolation] = []

    # Find all types: blocks
    i = 0
    while i < len(lines):
        block = find_types_block(lines, i)
        if block:
            types_blocks.append(block)
            i = block.end_line  # Skip past this block
        else:
            i += 1

    if not types_blocks:
        # No transformation needed
        return PreprocessResult(
            source=source,
            original_source=source,
            types_blocks=[],
            line_map={i: i for i in range(1, len(lines) + 1)},
        )

    # Validate types: blocks
    violations = validate_types_blocks(lines, types_blocks)

    # Transform the source
    result = transform_source(lines, types_blocks)
    result.violations = violations
    return result


def find_types_block(lines: list[str], start: int) -> TypesBlock | None:
    """Find a types: block starting at or after the given line index.

    Args:
        lines: Source lines.
        start: Line index to start searching from (0-indexed).

    Returns:
        TypesBlock if found, None otherwise.
    """
    # Look for 'types:' pattern
    types_pattern = re.compile(r'^(\s*)types:\s*$')

    for i in range(start, len(lines)):
        match = types_pattern.match(lines[i])
        if match:
            indent = match.group(1)
            block_indent_len = len(indent)

            # Find the extent of the indented block below
            declarations: list[tuple[str, int]] = []
            end_line = i + 1

            for j in range(i + 1, len(lines)):
                line = lines[j]

                # Empty lines are part of the block
                if not line.strip():
                    end_line = j + 1
                    continue

                # Check indentation
                line_indent = len(line) - len(line.lstrip())

                # Must be more indented than 'types:'
                if line_indent > block_indent_len:
                    # This is a declaration in the block
                    declarations.append((line, j + 1))  # 1-indexed line number
                    end_line = j + 1
                else:
                    # Block ended
                    break

            if declarations:
                return TypesBlock(
                    start_line=i + 1,  # 1-indexed
                    end_line=end_line,
                    indent=indent,
                    declarations=declarations,
                )

    return None


def transform_source(
    lines: list[str],
    types_blocks: list[TypesBlock],
) -> PreprocessResult:
    """Transform source by expanding types: blocks.

    Args:
        lines: Original source lines.
        types_blocks: List of types: blocks to transform.

    Returns:
        PreprocessResult with transformed source.
    """
    original_source = "\n".join(lines)
    new_lines: list[str] = []
    line_map: dict[int, int] = {}

    # Sort blocks by start line (should already be sorted, but be safe)
    blocks_by_line = {b.start_line: b for b in types_blocks}

    current_new_line = 1
    i = 0

    while i < len(lines):
        original_line_num = i + 1  # 1-indexed

        if original_line_num in blocks_by_line:
            block = blocks_by_line[original_line_num]

            # Skip the 'types:' line itself
            # Output declarations with reduced indentation
            for decl_text, decl_orig_line in block.declarations:
                # Reduce indentation by one level (4 spaces or 1 tab)
                transformed = reduce_indent(decl_text, block.indent)
                new_lines.append(transformed)
                line_map[current_new_line] = decl_orig_line
                current_new_line += 1

            # Skip to end of block
            i = block.end_line
        else:
            new_lines.append(lines[i])
            line_map[current_new_line] = original_line_num
            current_new_line += 1
            i += 1

    return PreprocessResult(
        source="\n".join(new_lines),
        original_source=original_source,
        types_blocks=types_blocks,
        line_map=line_map,
    )


def reduce_indent(line: str, base_indent: str) -> str:
    """Reduce indentation of a line by removing one level.

    The line should be indented more than base_indent.
    We remove enough to align with base_indent.

    Args:
        line: The line to transform.
        base_indent: The indentation of the 'types:' line.

    Returns:
        Line with reduced indentation.
    """
    if not line.strip():
        return line  # Empty lines stay empty

    # Find the current indentation
    stripped = line.lstrip()
    current_indent = line[:len(line) - len(stripped)]

    # The declaration is indented more than 'types:'
    # We want it at the same level as 'types:'
    # So just use base_indent
    return base_indent + stripped


def map_line_to_original(
    result: PreprocessResult,
    transformed_line: int,
) -> int:
    """Map a line number in transformed source to original source.

    Args:
        result: The preprocessing result.
        transformed_line: Line number in transformed source (1-indexed).

    Returns:
        Corresponding line number in original source (1-indexed).
    """
    return result.line_map.get(transformed_line, transformed_line)


def get_types_block_info(
    result: PreprocessResult,
    function_start_line: int,
) -> TypesBlock | None:
    """Get types: block info for a function.

    Args:
        result: The preprocessing result.
        function_start_line: Line where the function starts (in original source).

    Returns:
        TypesBlock if the function has one, None otherwise.
    """
    # Find block that starts after function_start_line
    # This is approximate - would need AST info for precision
    for block in result.types_blocks:
        if block.start_line > function_start_line:
            return block
    return None


def validate_types_blocks(
    lines: list[str],
    types_blocks: list[TypesBlock],
) -> list[PreprocessViolation]:
    """Validate types: block placement and uniqueness.

    Args:
        lines: Source lines (0-indexed).
        types_blocks: Found types: blocks.

    Returns:
        List of preprocessing violations.
    """
    violations: list[PreprocessViolation] = []

    # Group blocks by indent level (same indent = potentially same function context)
    blocks_by_indent: dict[int, list[TypesBlock]] = {}
    for block in types_blocks:
        indent_len = len(block.indent)
        if indent_len not in blocks_by_indent:
            blocks_by_indent[indent_len] = []
        blocks_by_indent[indent_len].append(block)

    # Track blocks that are duplicates (not the first at their indent level)
    duplicate_blocks: set[int] = set()  # start_line of duplicate blocks

    # Check for multiple blocks at same indent level
    for indent_len, blocks in blocks_by_indent.items():
        if len(blocks) > 1:
            # Report violations for all but the first
            for block in blocks[1:]:
                violations.append(PreprocessViolation(
                    line=block.start_line,
                    col=indent_len,
                    message="only one 'types:' block allowed per function",
                    code="XI006",
                ))
                duplicate_blocks.add(block.start_line)

    # Check each block is first statement (after optional docstring)
    # Skip duplicate blocks - they already have a violation
    for block in types_blocks:
        if block.start_line in duplicate_blocks:
            continue
        violation = check_types_block_position(lines, block)
        if violation:
            violations.append(violation)

    return violations


def check_types_block_position(
    lines: list[str],
    block: TypesBlock,
) -> PreprocessViolation | None:
    """Check if a types: block is the first statement in its function.

    Args:
        lines: Source lines (0-indexed).
        block: The types: block to check.

    Returns:
        A violation if the block is not first, None otherwise.
    """
    block_indent = block.indent
    block_indent_len = len(block_indent)

    # The types: block should be at function body indent level (e.g., 4 spaces inside def)
    # Look backwards to find the function definition
    func_def_line = None
    func_def_pattern = re.compile(r'^(\s*)def\s+\w+')

    for i in range(block.start_line - 2, -1, -1):  # start_line is 1-indexed
        line = lines[i]
        match = func_def_pattern.match(line)
        if match:
            func_indent_len = len(match.group(1))
            # Function body should be one indent level deeper than def
            expected_body_indent = func_indent_len + 4  # Assuming 4-space indent
            if block_indent_len == expected_body_indent:
                func_def_line = i
                break
        # If we hit a line with less or equal indent that's not empty, stop
        if line.strip() and not line.strip().startswith('#'):
            line_indent = len(line) - len(line.lstrip())
            if line_indent <= block_indent_len - 4:
                break

    if func_def_line is None:
        return None  # Can't find function, skip validation

    # Check lines between function def and types: block
    # Only docstrings and empty lines/comments are allowed
    docstring_pattern = re.compile(r'^\s*(\"\"\"|\'\'\').*')
    in_docstring = False
    docstring_quote = None

    for i in range(func_def_line + 1, block.start_line - 1):
        line = lines[i]
        stripped = line.strip()

        # Empty line or comment
        if not stripped or stripped.startswith('#'):
            continue

        # Handle docstrings
        if not in_docstring:
            # Check for docstring start
            for quote in ['"""', "'''"]:
                if stripped.startswith(quote):
                    in_docstring = True
                    docstring_quote = quote
                    # Check if docstring ends on same line
                    if stripped.count(quote) >= 2:
                        in_docstring = False
                    break
            else:
                # Not a docstring - this is a real statement before types:
                return PreprocessViolation(
                    line=block.start_line,
                    col=block_indent_len,
                    message="'types:' block must be first statement",
                    code="XI007",
                )
        else:
            # Inside docstring, check for end
            if docstring_quote and docstring_quote in stripped:
                in_docstring = False

    return None
