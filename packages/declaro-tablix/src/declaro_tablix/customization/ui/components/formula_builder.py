"""
Formula builder interface for TableV2 customization.

This module provides function-based HTMX component for building and
validating mathematical and logical formulas with visual assistance.
"""

import re
from typing import Any, Dict, List, Optional

from declaro_advise import error, success, warning
from declaro_tablix.customization.ui import UI_CONFIG, UI_ERRORS, UI_SUCCESS


def render_formula_builder(
    user_id: str,
    table_name: str,
    available_columns: List[Dict[str, Any]],
    current_formula: str = "",
    formula_type: str = "calculation",
    context: str = "column_creation",
    db_session=None,
) -> Dict[str, Any]:
    """
    Render the formula builder interface component.

    Args:
        user_id: User identifier
        table_name: Name of the table
        available_columns: List of available columns for reference
        current_formula: Current formula expression
        formula_type: Type of formula (calculation, condition, aggregation)
        context: Context where formula is being used
        db_session: Optional database session

    Returns:
        Component render result with HTML and metadata
    """
    try:
        # Build component data
        component_data = {
            "user_id": user_id,
            "table_name": table_name,
            "available_columns": available_columns,
            "current_formula": current_formula,
            "formula_type": formula_type,
            "context": context,
            "supported_functions": _get_supported_functions(formula_type),
            "supported_operators": _get_supported_operators(formula_type),
        }

        # Generate HTML content
        html_content = _generate_formula_builder_html(component_data)

        return {
            "success": True,
            "html": html_content,
            "component_type": "formula_builder",
            "metadata": component_data,
        }

    except Exception as e:
        error(f"Failed to render formula builder: {str(e)}")
        return {
            "success": False,
            "error": UI_ERRORS["render_error"],
            "html": _generate_error_html(str(e)),
        }


def validate_formula(
    formula: str,
    available_columns: List[Dict[str, Any]],
    formula_type: str = "calculation",
    db_session=None,
) -> Dict[str, Any]:
    """
    Validate a formula expression.

    Args:
        formula: Formula expression to validate
        available_columns: List of available columns
        formula_type: Type of formula being validated
        db_session: Optional database session

    Returns:
        Validation result with errors and suggestions
    """
    try:
        validation_result = {
            "success": True,
            "errors": [],
            "warnings": [],
            "suggestions": [],
            "parsed_tokens": [],
        }

        # Basic validation
        if not formula or not formula.strip():
            validation_result["errors"].append("Formula cannot be empty")
            validation_result["success"] = False
            return validation_result

        # Parse formula tokens
        tokens = _parse_formula_tokens(formula)
        validation_result["parsed_tokens"] = tokens

        # Validate syntax
        syntax_validation = _validate_syntax(formula, tokens, formula_type)
        validation_result["errors"].extend(syntax_validation["errors"])
        validation_result["warnings"].extend(syntax_validation["warnings"])

        # Validate column references
        column_validation = _validate_column_references(tokens, available_columns)
        validation_result["errors"].extend(column_validation["errors"])
        validation_result["warnings"].extend(column_validation["warnings"])

        # Validate functions
        function_validation = _validate_functions(tokens, formula_type)
        validation_result["errors"].extend(function_validation["errors"])
        validation_result["suggestions"].extend(function_validation["suggestions"])

        # Set overall success
        validation_result["success"] = len(validation_result["errors"]) == 0

        return validation_result

    except Exception as e:
        error(f"Failed to validate formula: {str(e)}")
        return {
            "success": False,
            "errors": [f"Validation error: {str(e)}"],
            "warnings": [],
            "suggestions": [],
        }


def get_formula_suggestions(
    partial_formula: str,
    cursor_position: int,
    available_columns: List[Dict[str, Any]],
    formula_type: str = "calculation",
) -> Dict[str, Any]:
    """
    Get autocomplete suggestions for formula building.

    Args:
        partial_formula: Partial formula text
        cursor_position: Current cursor position
        available_columns: List of available columns
        formula_type: Type of formula

    Returns:
        List of suggestions with metadata
    """
    try:
        suggestions = []

        # Get context around cursor
        context = _get_cursor_context(partial_formula, cursor_position)

        # Column suggestions
        if context["expecting_column"]:
            for col in available_columns:
                if context["prefix"].lower() in col["name"].lower():
                    suggestions.append(
                        {
                            "type": "column",
                            "value": f"{{{col['name']}}}",
                            "display": col["name"],
                            "description": f"Column: {col.get('data_type', 'unknown')}",
                            "icon": "fas fa-columns",
                        }
                    )

        # Function suggestions
        if context["expecting_function"]:
            functions = _get_supported_functions(formula_type)
            for func_name, func_info in functions.items():
                if context["prefix"].lower() in func_name.lower():
                    suggestions.append(
                        {
                            "type": "function",
                            "value": func_info["signature"],
                            "display": func_name,
                            "description": func_info["description"],
                            "icon": "fas fa-function",
                        }
                    )

        # Operator suggestions
        if context["expecting_operator"]:
            operators = _get_supported_operators(formula_type)
            for op in operators:
                if context["prefix"].lower() in op["symbol"].lower():
                    suggestions.append(
                        {
                            "type": "operator",
                            "value": op["symbol"],
                            "display": op["symbol"],
                            "description": op["description"],
                            "icon": "fas fa-calculator",
                        }
                    )

        return {
            "success": True,
            "suggestions": suggestions[:10],  # Limit to 10 suggestions
            "context": context,
        }

    except Exception as e:
        error(f"Failed to get formula suggestions: {str(e)}")
        return {"success": False, "suggestions": [], "error": str(e)}


def _generate_formula_builder_html(component_data: Dict[str, Any]) -> str:
    """Generate HTML for the formula builder interface."""
    user_id = component_data["user_id"]
    table_name = component_data["table_name"]
    available_columns = component_data["available_columns"]
    current_formula = component_data["current_formula"]
    formula_type = component_data["formula_type"]
    supported_functions = component_data["supported_functions"]
    supported_operators = component_data["supported_operators"]

    # Generate column options
    columns_html = "\n".join(
        [
            f'<div class="column-item" data-column-id="{col["id"]}" data-column-name="{col["name"]}" data-type="{col.get("data_type", "text")}">'
            f'<i class="fas fa-columns me-2"></i>{col["name"]} <small class="text-muted">({col.get("data_type", "text")})</small>'
            f"</div>"
            for col in available_columns
        ]
    )

    # Generate function options
    functions_html = "\n".join(
        [
            f'<div class="function-item" data-function="{name}" data-signature="{info["signature"]}">'
            f'<i class="fas fa-function me-2"></i>{name} <small class="text-muted">{info["description"]}</small>'
            f"</div>"
            for name, info in supported_functions.items()
        ]
    )

    # Generate operator options
    operators_html = "\n".join(
        [
            f'<div class="operator-item" data-operator="{op["symbol"]}">'
            f'<i class="fas fa-calculator me-2"></i>{op["symbol"]} <small class="text-muted">{op["description"]}</small>'
            f"</div>"
            for op in supported_operators
        ]
    )

    return f"""
    <div class="formula-builder-container">
        <div class="row">
            <!-- Formula Editor -->
            <div class="col-md-8">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-edit me-2"></i>Formula Editor</h6>
                    </div>
                    <div class="card-body">
                        <div class="mb-3">
                            <label for="formulaEditor" class="form-label fw-bold">
                                Formula Expression <span class="text-danger">*</span>
                            </label>
                            <div class="formula-editor-wrapper">
                                <textarea class="form-control font-monospace formula-editor"
                                          id="formulaEditor"
                                          name="formula"
                                          rows="6"
                                          placeholder="Enter your formula..."
                                          hx-trigger="keyup changed delay:500ms"
                                          hx-post="/api/tableV2/customization/validate-formula"
                                          hx-target="#formulaValidation"
                                          hx-indicator="#validationSpinner"
                                          data-formula-type="{formula_type}"
                                          autocomplete="off">{current_formula}</textarea>

                                <!-- Autocomplete suggestions -->
                                <div id="formulaSuggestions" class="formula-suggestions"></div>
                            </div>

                            <div class="form-text">
                                Use column names in curly braces: {{column_name}}<br>
                                Press Ctrl+Space for autocomplete suggestions
                            </div>
                        </div>

                        <!-- Formula validation results -->
                        <div id="formulaValidation" class="mb-3"></div>
                        <div id="validationSpinner" class="htmx-indicator">
                            <i class="fas fa-spinner fa-spin"></i> Validating formula...
                        </div>

                        <!-- Formula preview -->
                        <div class="mb-3">
                            <label class="form-label fw-bold">Formula Preview</label>
                            <div class="card bg-light">
                                <div class="card-body">
                                    <div id="formulaPreview" class="font-monospace">
                                        {current_formula if current_formula else 'Enter a formula to see preview...'}
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Test formula -->
                        <div class="mb-3">
                            <button type="button"
                                    class="btn btn-outline-primary"
                                    onclick="testFormula()"
                                    id="testFormulaBtn">
                                <i class="fas fa-play me-2"></i>Test Formula
                            </button>
                            <div id="testResults" class="mt-2"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Formula Helpers -->
            <div class="col-md-4">
                <!-- Columns -->
                <div class="card mb-3">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-columns me-2"></i>Available Columns</h6>
                    </div>
                    <div class="card-body p-2" style="max-height: 200px; overflow-y: auto;">
                        <div class="formula-helper-items">
                            {columns_html}
                        </div>
                    </div>
                </div>

                <!-- Functions -->
                <div class="card mb-3">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-function me-2"></i>Functions</h6>
                    </div>
                    <div class="card-body p-2" style="max-height: 200px; overflow-y: auto;">
                        <div class="formula-helper-items">
                            {functions_html}
                        </div>
                    </div>
                </div>

                <!-- Operators -->
                <div class="card mb-3">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-calculator me-2"></i>Operators</h6>
                    </div>
                    <div class="card-body p-2" style="max-height: 150px; overflow-y: auto;">
                        <div class="formula-helper-items">
                            {operators_html}
                        </div>
                    </div>
                </div>

                <!-- Common Patterns -->
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-lightbulb me-2"></i>Common Patterns</h6>
                    </div>
                    <div class="card-body p-2">
                        <div class="d-grid gap-1">
                            <button type="button" class="btn btn-sm btn-outline-secondary text-start"
                                    onclick="insertPattern('IF({{condition}}, {{true_value}}, {{false_value}})')">
                                Conditional Logic
                            </button>
                            <button type="button" class="btn btn-sm btn-outline-secondary text-start"
                                    onclick="insertPattern('ROUND({{value}}, 2)')">
                                Round Number
                            </button>
                            <button type="button" class="btn btn-sm btn-outline-secondary text-start"
                                    onclick="insertPattern('CONCAT({{text1}}, \\\" \\\", {{text2}})')">
                                Concatenate Text
                            </button>
                            <button type="button" class="btn btn-sm btn-outline-secondary text-start"
                                    onclick="insertPattern('{{value1}} + {{value2}}')">
                                Addition
                            </button>
                            <button type="button" class="btn btn-sm btn-outline-secondary text-start"
                                    onclick="insertPattern('{{value}} * 0.01')">
                                Percentage
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <input type="hidden" name="user_id" value="{user_id}">
        <input type="hidden" name="table_name" value="{table_name}">
        <input type="hidden" name="formula_type" value="{formula_type}">
    </div>

    <style>
        .formula-builder-container {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        }}

        .formula-editor {{
            line-height: 1.5;
            tab-size: 4;
        }}

        .formula-editor-wrapper {{
            position: relative;
        }}

        .formula-suggestions {{
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: white;
            border: 1px solid #dee2e6;
            border-top: none;
            border-radius: 0 0 0.375rem 0.375rem;
            max-height: 200px;
            overflow-y: auto;
            z-index: 1000;
            display: none;
        }}

        .suggestion-item {{
            padding: 8px 12px;
            cursor: pointer;
            border-bottom: 1px solid #f8f9fa;
        }}

        .suggestion-item:hover,
        .suggestion-item.active {{
            background-color: #e9ecef;
        }}

        .formula-helper-items .column-item,
        .formula-helper-items .function-item,
        .formula-helper-items .operator-item {{
            padding: 4px 8px;
            cursor: pointer;
            border-radius: 0.25rem;
            margin-bottom: 2px;
            font-size: 0.875rem;
        }}

        .formula-helper-items .column-item:hover,
        .formula-helper-items .function-item:hover,
        .formula-helper-items .operator-item:hover {{
            background-color: #e9ecef;
        }}

        .htmx-indicator {{
            display: none;
        }}

        .htmx-request .htmx-indicator {{
            display: block;
        }}
    </style>

    <script>
        // Formula builder JavaScript
        class FormulaBuilder {{
            constructor() {{
                this.editor = document.getElementById('formulaEditor');
                this.suggestions = document.getElementById('formulaSuggestions');
                this.preview = document.getElementById('formulaPreview');
                this.currentSuggestions = [];
                this.selectedSuggestionIndex = -1;

                this.initializeEventListeners();
            }}

            initializeEventListeners() {{
                // Editor events
                this.editor.addEventListener('input', () => this.updatePreview());
                this.editor.addEventListener('keydown', (e) => this.handleKeyDown(e));
                this.editor.addEventListener('keyup', (e) => this.handleKeyUp(e));
                this.editor.addEventListener('blur', () => this.hideSuggestions());

                // Helper item clicks
                document.querySelectorAll('.column-item').forEach(item => {{
                    item.addEventListener('click', () => this.insertColumn(item.dataset.columnName));
                }});

                document.querySelectorAll('.function-item').forEach(item => {{
                    item.addEventListener('click', () => this.insertFunction(item.dataset.signature));
                }});

                document.querySelectorAll('.operator-item').forEach(item => {{
                    item.addEventListener('click', () => this.insertOperator(item.dataset.operator));
                }});
            }}

            updatePreview() {{
                const formula = this.editor.value;
                this.preview.textContent = formula || 'Enter a formula to see preview...';
                this.preview.className = formula ? 'font-monospace text-dark' : 'font-monospace text-muted';
            }}

            handleKeyDown(e) {{
                if (e.ctrlKey && e.key === ' ') {{
                    e.preventDefault();
                    this.showSuggestions();
                }} else if (this.suggestions.style.display === 'block') {{
                    if (e.key === 'ArrowDown') {{
                        e.preventDefault();
                        this.selectNextSuggestion();
                    }} else if (e.key === 'ArrowUp') {{
                        e.preventDefault();
                        this.selectPreviousSuggestion();
                    }} else if (e.key === 'Enter' && this.selectedSuggestionIndex >= 0) {{
                        e.preventDefault();
                        this.applySuggestion(this.currentSuggestions[this.selectedSuggestionIndex]);
                    }} else if (e.key === 'Escape') {{
                        this.hideSuggestions();
                    }}
                }}
            }}

            handleKeyUp(e) {{
                if (!e.ctrlKey && !['ArrowDown', 'ArrowUp', 'Enter', 'Escape'].includes(e.key)) {{
                    this.updateSuggestions();
                }}
            }}

            showSuggestions() {{
                // Implementation for showing autocomplete suggestions
                this.updateSuggestions();
            }}

            updateSuggestions() {{
                const cursorPos = this.editor.selectionStart;
                const formula = this.editor.value;

                // Get suggestions from server
                htmx.ajax('POST', '/api/tableV2/customization/get-formula-suggestions', {{
                    values: {{
                        partial_formula: formula,
                        cursor_position: cursorPos,
                        formula_type: '{formula_type}',
                        table_name: '{table_name}'
                    }},
                    swap: 'none'
                }}).then(response => {{
                    // Handle suggestions response
                    this.displaySuggestions(response.suggestions || []);
                }});
            }}

            displaySuggestions(suggestions) {{
                this.currentSuggestions = suggestions;
                this.selectedSuggestionIndex = -1;

                if (suggestions.length === 0) {{
                    this.hideSuggestions();
                    return;
                }}

                const html = suggestions.map((suggestion, index) => {{
                    return `
                        <div class="suggestion-item" data-index="${{index}}">
                            <i class="${{suggestion.icon}} me-2"></i>
                            <strong>${{suggestion.display}}</strong>
                            <div class="text-muted small">${{suggestion.description}}</div>
                        </div>
                    `;
                }}).join('');

                this.suggestions.innerHTML = html;
                this.suggestions.style.display = 'block';

                // Add click listeners to suggestions
                this.suggestions.querySelectorAll('.suggestion-item').forEach(item => {{
                    item.addEventListener('click', () => {{
                        const index = parseInt(item.dataset.index);
                        this.applySuggestion(this.currentSuggestions[index]);
                    }});
                }});
            }}

            selectNextSuggestion() {{
                if (this.selectedSuggestionIndex < this.currentSuggestions.length - 1) {{
                    this.selectedSuggestionIndex++;
                    this.updateSuggestionSelection();
                }}
            }}

            selectPreviousSuggestion() {{
                if (this.selectedSuggestionIndex > 0) {{
                    this.selectedSuggestionIndex--;
                    this.updateSuggestionSelection();
                }}
            }}

            updateSuggestionSelection() {{
                this.suggestions.querySelectorAll('.suggestion-item').forEach((item, index) => {{
                    item.classList.toggle('active', index === this.selectedSuggestionIndex);
                }});
            }}

            applySuggestion(suggestion) {{
                const cursorPos = this.editor.selectionStart;
                const formula = this.editor.value;

                // Insert suggestion at cursor position
                const before = formula.substring(0, cursorPos);
                const after = formula.substring(cursorPos);

                this.editor.value = before + suggestion.value + after;
                this.editor.focus();

                // Set cursor after inserted text
                const newPos = cursorPos + suggestion.value.length;
                this.editor.setSelectionRange(newPos, newPos);

                this.hideSuggestions();
                this.updatePreview();
            }}

            hideSuggestions() {{
                this.suggestions.style.display = 'none';
                this.currentSuggestions = [];
                this.selectedSuggestionIndex = -1;
            }}

            insertColumn(columnName) {{
                this.insertText(`{{${{columnName}}}}`);
            }}

            insertFunction(signature) {{
                this.insertText(signature);
            }}

            insertOperator(operator) {{
                this.insertText(` ${{operator}} `);
            }}

            insertText(text) {{
                const cursorPos = this.editor.selectionStart;
                const formula = this.editor.value;
                const before = formula.substring(0, cursorPos);
                const after = formula.substring(cursorPos);

                this.editor.value = before + text + after;
                this.editor.focus();

                const newPos = cursorPos + text.length;
                this.editor.setSelectionRange(newPos, newPos);

                this.updatePreview();
            }}
        }}

        // Global functions
        function insertPattern(pattern) {{
            if (window.formulaBuilder) {{
                window.formulaBuilder.insertText(pattern);
            }}
        }}

        function testFormula() {{
            const formula = document.getElementById('formulaEditor').value;
            if (!formula.trim()) {{
                alert('Please enter a formula to test');
                return;
            }}

            htmx.ajax('POST', '/api/tableV2/customization/test-formula', {{
                values: {{
                    formula: formula,
                    table_name: '{table_name}',
                    formula_type: '{formula_type}'
                }},
                target: '#testResults'
            }});
        }}

        // Initialize formula builder when DOM is ready
        document.addEventListener('DOMContentLoaded', function() {{
            window.formulaBuilder = new FormulaBuilder();
        }});
    </script>
    """


def _get_supported_functions(formula_type: str) -> Dict[str, Dict[str, str]]:
    """Get supported functions for the formula type."""
    base_functions = {
        "ABS": {"signature": "ABS({value})", "description": "Absolute value"},
        "ROUND": {"signature": "ROUND({value}, {decimals})", "description": "Round to specified decimals"},
        "CEILING": {"signature": "CEILING({value})", "description": "Round up to nearest integer"},
        "FLOOR": {"signature": "FLOOR({value})", "description": "Round down to nearest integer"},
        "IF": {"signature": "IF({condition}, {true_value}, {false_value})", "description": "Conditional logic"},
        "CONCAT": {"signature": "CONCAT({text1}, {text2})", "description": "Concatenate text"},
        "LENGTH": {"signature": "LENGTH({text})", "description": "Text length"},
        "UPPER": {"signature": "UPPER({text})", "description": "Convert to uppercase"},
        "LOWER": {"signature": "LOWER({text})", "description": "Convert to lowercase"},
        "TRIM": {"signature": "TRIM({text})", "description": "Remove whitespace"},
    }

    if formula_type == "aggregation":
        base_functions.update(
            {
                "SUM": {"signature": "SUM({values})", "description": "Sum of values"},
                "AVG": {"signature": "AVG({values})", "description": "Average of values"},
                "COUNT": {"signature": "COUNT({values})", "description": "Count of values"},
                "MIN": {"signature": "MIN({values})", "description": "Minimum value"},
                "MAX": {"signature": "MAX({values})", "description": "Maximum value"},
            }
        )

    return base_functions


def _get_supported_operators(formula_type: str) -> List[Dict[str, str]]:
    """Get supported operators for the formula type."""
    operators = [
        {"symbol": "+", "description": "Addition"},
        {"symbol": "-", "description": "Subtraction"},
        {"symbol": "*", "description": "Multiplication"},
        {"symbol": "/", "description": "Division"},
        {"symbol": "=", "description": "Equal to"},
        {"symbol": "!=", "description": "Not equal to"},
        {"symbol": ">", "description": "Greater than"},
        {"symbol": "<", "description": "Less than"},
        {"symbol": ">=", "description": "Greater than or equal"},
        {"symbol": "<=", "description": "Less than or equal"},
    ]

    if formula_type == "condition":
        operators.extend(
            [
                {"symbol": "AND", "description": "Logical AND"},
                {"symbol": "OR", "description": "Logical OR"},
                {"symbol": "NOT", "description": "Logical NOT"},
            ]
        )

    return operators


def _parse_formula_tokens(formula: str) -> List[Dict[str, str]]:
    """Parse formula into tokens for validation."""
    tokens = []

    # Regular expressions for different token types
    patterns = {
        "column": r"\{([^}]+)\}",
        "function": r"([A-Z_]+)\s*\(",
        "number": r"\b\d+\.?\d*\b",
        "string": r"[\"']([^\"']*)[\"']",
        "operator": r"[+\-*/=<>!]+|AND|OR|NOT",
        "parenthesis": r"[()]",
        "comma": r",",
        "whitespace": r"\s+",
    }

    position = 0
    while position < len(formula):
        matched = False

        for token_type, pattern in patterns.items():
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.match(formula, position)

            if match:
                if token_type != "whitespace":  # Skip whitespace tokens
                    tokens.append(
                        {
                            "type": token_type,
                            "value": match.group(0),
                            "position": position,
                            "length": len(match.group(0)),
                        }
                    )

                position = match.end()
                matched = True
                break

        if not matched:
            # Unknown character
            tokens.append(
                {
                    "type": "unknown",
                    "value": formula[position],
                    "position": position,
                    "length": 1,
                }
            )
            position += 1

    return tokens


def _validate_syntax(formula: str, tokens: List[Dict[str, str]], formula_type: str) -> Dict[str, List[str]]:
    """Validate formula syntax."""
    errors = []
    warnings = []

    # Check parentheses balance
    paren_count = 0
    for token in tokens:
        if token["value"] == "(":
            paren_count += 1
        elif token["value"] == ")":
            paren_count -= 1
            if paren_count < 0:
                errors.append("Mismatched closing parenthesis")
                break

    if paren_count > 0:
        errors.append("Unclosed parentheses")
    elif paren_count < 0:
        errors.append("Extra closing parentheses")

    # Check for unknown tokens
    unknown_tokens = [t for t in tokens if t["type"] == "unknown"]
    if unknown_tokens:
        errors.append(f"Unknown characters: {', '.join([t['value'] for t in unknown_tokens])}")

    return {"errors": errors, "warnings": warnings}


def _validate_column_references(
    tokens: List[Dict[str, str]], available_columns: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """Validate column references in formula."""
    errors = []
    warnings = []

    column_names = {col["name"].lower() for col in available_columns}
    column_tokens = [t for t in tokens if t["type"] == "column"]

    for token in column_tokens:
        # Extract column name from {column_name}
        column_name = token["value"][1:-1]  # Remove { and }

        if column_name.lower() not in column_names:
            errors.append(f"Unknown column: {column_name}")

    return {"errors": errors, "warnings": warnings}


def _validate_functions(tokens: List[Dict[str, str]], formula_type: str) -> Dict[str, List[str]]:
    """Validate function usage in formula."""
    errors = []
    suggestions = []

    supported_functions = _get_supported_functions(formula_type)
    function_tokens = [t for t in tokens if t["type"] == "function"]

    for token in function_tokens:
        # Extract function name (remove opening parenthesis)
        func_name = token["value"].rstrip("(").strip()

        if func_name.upper() not in supported_functions:
            errors.append(f"Unknown function: {func_name}")

            # Suggest similar functions
            similar = [name for name in supported_functions.keys() if name.lower().startswith(func_name.lower()[:2])]
            if similar:
                suggestions.append(f"Did you mean: {', '.join(similar)}?")

    return {"errors": errors, "suggestions": suggestions}


def _get_cursor_context(formula: str, cursor_position: int) -> Dict[str, Any]:
    """Analyze context around cursor for autocomplete."""
    # Get text before cursor
    text_before = formula[:cursor_position]

    # Find the current word/token being typed
    word_start = cursor_position
    while word_start > 0 and formula[word_start - 1].isalnum():
        word_start -= 1

    current_word = formula[word_start:cursor_position]

    # Determine what type of suggestion is expected
    expecting_column = "{" in text_before and "}" not in text_before[text_before.rfind("{") :]
    expecting_function = text_before.strip().endswith("(") or current_word.isupper()
    expecting_operator = (
        not expecting_column and not expecting_function and current_word in ["+", "-", "*", "/", "=", "<", ">"]
    )

    return {
        "prefix": current_word,
        "expecting_column": expecting_column,
        "expecting_function": expecting_function,
        "expecting_operator": expecting_operator,
        "text_before": text_before,
        "word_start": word_start,
    }


def _generate_error_html(error_message: str) -> str:
    """Generate error HTML for failed component renders."""
    return f"""
    <div class="alert alert-danger" role="alert">
        <i class="fas fa-exclamation-triangle me-2"></i>
        <strong>Error:</strong> {error_message}
    </div>
    """


# Export all functions
__all__ = [
    "render_formula_builder",
    "validate_formula",
    "get_formula_suggestions",
]
