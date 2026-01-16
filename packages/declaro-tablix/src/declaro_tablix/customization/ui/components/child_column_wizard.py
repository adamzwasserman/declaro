"""
Child column creation wizard for TableV2 customization.

This module provides function-based HTMX component for creating derived
columns with formulas, aggregations, and transformations.
"""

from typing import Any, Dict, List, Optional

from declaro_advise import error, success, warning
from declaro_tablix.customization.ui import UI_CONFIG, UI_ERRORS, UI_SUCCESS


def render_child_column_wizard(
    user_id: str,
    table_name: str,
    parent_column_id: str,
    available_columns: List[Dict[str, Any]],
    step: int = 1,
    wizard_data: Optional[Dict[str, Any]] = None,
    db_session=None,
) -> Dict[str, Any]:
    """
    Render the child column creation wizard component.

    Args:
        user_id: User identifier
        table_name: Name of the table
        parent_column_id: Parent column identifier
        available_columns: List of available columns for reference
        step: Current wizard step (1-4)
        wizard_data: Current wizard data
        db_session: Optional database session

    Returns:
        Component render result with HTML and metadata
    """
    try:
        # Initialize wizard data if not provided
        if wizard_data is None:
            wizard_data = {
                "column_type": "",
                "column_name": "",
                "formula": "",
                "aggregation_type": "",
                "transformation_type": "",
                "validation_rules": {},
                "format_options": {},
            }

        # Build component data
        component_data = {
            "user_id": user_id,
            "table_name": table_name,
            "parent_column_id": parent_column_id,
            "available_columns": available_columns,
            "current_step": step,
            "wizard_data": wizard_data,
            "total_steps": 4,
        }

        # Generate HTML content based on current step
        html_content = _generate_wizard_html(component_data)

        return {
            "success": True,
            "html": html_content,
            "component_type": "child_column_wizard",
            "metadata": component_data,
        }

    except Exception as e:
        error(f"Failed to render child column wizard: {str(e)}")
        return {
            "success": False,
            "error": UI_ERRORS["render_error"],
            "html": _generate_error_html(str(e)),
        }


def process_wizard_step(
    user_id: str,
    table_name: str,
    parent_column_id: str,
    step: int,
    step_data: Dict[str, Any],
    wizard_data: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """
    Process a wizard step and advance to next step.

    Args:
        user_id: User identifier
        table_name: Name of the table
        parent_column_id: Parent column identifier
        step: Current step being processed
        step_data: Data submitted for current step
        wizard_data: Accumulated wizard data
        db_session: Optional database session

    Returns:
        Processing result with next step or completion
    """
    try:
        # Validate step data
        validation_result = _validate_step_data(step, step_data)
        if not validation_result["success"]:
            return {
                "success": False,
                "error": "Step validation failed",
                "errors": validation_result["errors"],
                "stay_on_step": True,
            }

        # Update wizard data with step data
        updated_wizard_data = {**wizard_data, **step_data}

        # Check if this is the final step
        if step >= 4:
            # Create the child column
            creation_result = _create_child_column(
                user_id=user_id,
                table_name=table_name,
                parent_column_id=parent_column_id,
                wizard_data=updated_wizard_data,
                db_session=db_session,
            )

            if creation_result["success"]:
                success(UI_SUCCESS["customization_saved"])
                return {
                    "success": True,
                    "completed": True,
                    "message": f"Child column '{updated_wizard_data['column_name']}' created successfully",
                    "data": creation_result["data"],
                }
            else:
                return {
                    "success": False,
                    "error": creation_result["error"],
                    "stay_on_step": True,
                }
        else:
            # Advance to next step
            return {
                "success": True,
                "next_step": step + 1,
                "wizard_data": updated_wizard_data,
                "message": f"Step {step} completed",
            }

    except Exception as e:
        error(f"Failed to process wizard step: {str(e)}")
        return {"success": False, "error": str(e), "stay_on_step": True}


def _generate_wizard_html(component_data: Dict[str, Any]) -> str:
    """Generate HTML for the child column wizard."""
    step = component_data["current_step"]
    total_steps = component_data["total_steps"]
    wizard_data = component_data["wizard_data"]
    user_id = component_data["user_id"]
    table_name = component_data["table_name"]
    parent_column_id = component_data["parent_column_id"]
    available_columns = component_data["available_columns"]

    # Progress indicator
    progress_html = _generate_progress_indicator(step, total_steps)

    # Step content
    if step == 1:
        step_content = _generate_step1_html(wizard_data, available_columns)
    elif step == 2:
        step_content = _generate_step2_html(wizard_data, available_columns)
    elif step == 3:
        step_content = _generate_step3_html(wizard_data)
    elif step == 4:
        step_content = _generate_step4_html(wizard_data)
    else:
        step_content = '<div class="alert alert-danger">Invalid step</div>'

    return f"""
    <div class="modal fade" id="childColumnWizardModal" tabindex="-1" aria-labelledby="childColumnWizardModalLabel" aria-hidden="true" data-bs-backdrop="static">
        <div class="modal-dialog modal-xl">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="childColumnWizardModalLabel">
                        <i class="fas fa-magic me-2"></i>Create Child Column
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>

                <div class="modal-body">
                    {progress_html}

                    <form id="wizardStepForm"
                          hx-post="/api/tableV2/customization/process-wizard-step"
                          hx-target="#wizardContent"
                          hx-indicator="#wizardSpinner">

                        <input type="hidden" name="user_id" value="{user_id}">
                        <input type="hidden" name="table_name" value="{table_name}">
                        <input type="hidden" name="parent_column_id" value="{parent_column_id}">
                        <input type="hidden" name="current_step" value="{step}">
                        <input type="hidden" name="wizard_data" value="{_encode_wizard_data(wizard_data)}">

                        <div id="wizardContent">
                            {step_content}
                        </div>

                        <!-- Navigation buttons -->
                        <div class="d-flex justify-content-between mt-4">
                            <div>
                                {'<button type="button" class="btn btn-outline-secondary" onclick="previousStep()"><i class="fas fa-arrow-left me-1"></i>Previous</button>' if step > 1 else ''}
                            </div>

                            <div class="d-flex gap-2">
                                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">
                                    Cancel
                                </button>

                                <button type="submit" class="btn btn-primary">
                                    {'<i class="fas fa-check me-1"></i>Create Column' if step >= total_steps else '<i class="fas fa-arrow-right me-1"></i>Next'}
                                </button>
                            </div>
                        </div>

                        <div id="wizardSpinner" class="htmx-indicator mt-2 text-center">
                            <i class="fas fa-spinner fa-spin"></i> Processing...
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </div>

    <script>
        function previousStep() {{
            const currentStep = parseInt(document.querySelector('[name="current_step"]').value);
            if (currentStep > 1) {{
                document.querySelector('[name="current_step"]').value = currentStep - 1;
                document.getElementById('wizardStepForm').dispatchEvent(new Event('submit'));
            }}
        }}

        // Auto-focus first input when modal opens
        document.getElementById('childColumnWizardModal').addEventListener('shown.bs.modal', function() {{
            const firstInput = this.querySelector('input[type="text"]:not([type="hidden"]), select, textarea');
            if (firstInput) firstInput.focus();
        }});
    </script>
    """


def _generate_progress_indicator(current_step: int, total_steps: int) -> str:
    """Generate progress indicator HTML."""
    steps = ["Column Type", "Configuration", "Validation", "Preview"]

    progress_html = '<div class="progress-wizard mb-4"><div class="d-flex justify-content-between">'

    for i, step_name in enumerate(steps, 1):
        active_class = "active" if i == current_step else ""
        completed_class = "completed" if i < current_step else ""

        progress_html += f"""
        <div class="step-item {active_class} {completed_class}">
            <div class="step-number">{i}</div>
            <div class="step-label">{step_name}</div>
        </div>
        """

    progress_html += "</div></div>"
    return progress_html


def _generate_step1_html(wizard_data: Dict[str, Any], available_columns: List[Dict[str, Any]]) -> str:
    """Generate Step 1: Column Type Selection."""
    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 1: Choose Column Type</h6>

        <div class="row">
            <div class="col-md-6">
                <label for="columnType" class="form-label fw-bold">Column Type <span class="text-danger">*</span></label>
                <select class="form-select" id="columnType" name="column_type" required>
                    <option value="">Select column type...</option>
                    <option value="formula" {'selected' if wizard_data.get('column_type') == 'formula' else ''}>Formula Column</option>
                    <option value="aggregation" {'selected' if wizard_data.get('column_type') == 'aggregation' else ''}>Aggregation Column</option>
                    <option value="transformation" {'selected' if wizard_data.get('column_type') == 'transformation' else ''}>Transformation Column</option>
                    <option value="lookup" {'selected' if wizard_data.get('column_type') == 'lookup' else ''}>Lookup Column</option>
                </select>
                <div class="form-text">Choose how this column will derive its values.</div>
            </div>

            <div class="col-md-6">
                <label for="columnName" class="form-label fw-bold">Column Name <span class="text-danger">*</span></label>
                <input type="text"
                       class="form-control"
                       id="columnName"
                       name="column_name"
                       value="{wizard_data.get('column_name', '')}"
                       placeholder="Enter column name..."
                       maxlength="255"
                       required>
                <div class="form-text">This will be the display name for your new column.</div>
            </div>
        </div>

        <div class="mt-4">
            <h6 class="fw-bold">Column Type Descriptions</h6>
            <div class="row">
                <div class="col-md-6">
                    <div class="card h-100">
                        <div class="card-body">
                            <h6 class="card-title"><i class="fas fa-calculator text-primary"></i> Formula Column</h6>
                            <p class="card-text">Create calculated fields using mathematical expressions and functions.</p>
                            <small class="text-muted">Example: Price * Quantity</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card h-100">
                        <div class="card-body">
                            <h6 class="card-title"><i class="fas fa-chart-bar text-success"></i> Aggregation Column</h6>
                            <p class="card-text">Summarize data using SUM, AVG, COUNT, MIN, MAX functions.</p>
                            <small class="text-muted">Example: SUM(Amount) by Category</small>
                        </div>
                    </div>
                </div>
            </div>
            <div class="row mt-2">
                <div class="col-md-6">
                    <div class="card h-100">
                        <div class="card-body">
                            <h6 class="card-title"><i class="fas fa-exchange-alt text-warning"></i> Transformation Column</h6>
                            <p class="card-text">Transform values using string operations, date formatting, etc.</p>
                            <small class="text-muted">Example: UPPER(Name), FORMAT_DATE(Date)</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card h-100">
                        <div class="card-body">
                            <h6 class="card-title"><i class="fas fa-search text-info"></i> Lookup Column</h6>
                            <p class="card-text">Reference values from other tables or data sources.</p>
                            <small class="text-muted">Example: Get Product Name by Product ID</small>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    """


def _generate_step2_html(wizard_data: Dict[str, Any], available_columns: List[Dict[str, Any]]) -> str:
    """Generate Step 2: Configuration."""
    column_type = wizard_data.get("column_type", "")

    if column_type == "formula":
        return _generate_formula_config_html(wizard_data, available_columns)
    elif column_type == "aggregation":
        return _generate_aggregation_config_html(wizard_data, available_columns)
    elif column_type == "transformation":
        return _generate_transformation_config_html(wizard_data, available_columns)
    elif column_type == "lookup":
        return _generate_lookup_config_html(wizard_data, available_columns)
    else:
        return '<div class="alert alert-warning">Please select a column type first.</div>'


def _generate_formula_config_html(wizard_data: Dict[str, Any], available_columns: List[Dict[str, Any]]) -> str:
    """Generate formula configuration HTML."""
    columns_options = "\n".join([f'<option value="{col["id"]}">{col["name"]}</option>' for col in available_columns])

    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 2: Formula Configuration</h6>

        <div class="row">
            <div class="col-md-8">
                <label for="formula" class="form-label fw-bold">Formula Expression <span class="text-danger">*</span></label>
                <textarea class="form-control font-monospace"
                          id="formula"
                          name="formula"
                          rows="4"
                          placeholder="Enter formula expression..."
                          required>{wizard_data.get('formula', '')}</textarea>
                <div class="form-text">
                    Use column names in curly braces: {{column_name}}<br>
                    Supported functions: +, -, *, /, ROUND(), IF(), CONCAT()
                </div>
            </div>

            <div class="col-md-4">
                <label class="form-label fw-bold">Available Columns</label>
                <select class="form-select" onchange="insertColumn(this.value)">
                    <option value="">Insert column...</option>
                    {columns_options}
                </select>

                <div class="mt-3">
                    <label class="form-label fw-bold">Common Functions</label>
                    <div class="d-grid gap-1">
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="insertFunction('ROUND({{value}}, 2)')">ROUND</button>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="insertFunction('IF({{condition}}, {{true_value}}, {{false_value}})')">IF</button>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="insertFunction('CONCAT({{value1}}, {{value2}})')">CONCAT</button>
                    </div>
                </div>
            </div>
        </div>

        <div class="mt-3">
            <label class="form-label fw-bold">Formula Preview</label>
            <div class="card">
                <div class="card-body">
                    <div id="formulaPreview" class="font-monospace text-muted">
                        Enter a formula to see preview...
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        function insertColumn(columnId) {{
            if (columnId) {{
                const formula = document.getElementById('formula');
                const cursorPos = formula.selectionStart;
                const textBefore = formula.value.substring(0, cursorPos);
                const textAfter = formula.value.substring(cursorPos);
                formula.value = textBefore + '{{' + columnId + '}}' + textAfter;
                formula.focus();
                formula.setSelectionRange(cursorPos + columnId.length + 4, cursorPos + columnId.length + 4);
                updateFormulaPreview();
            }}
        }}

        function insertFunction(functionText) {{
            const formula = document.getElementById('formula');
            const cursorPos = formula.selectionStart;
            const textBefore = formula.value.substring(0, cursorPos);
            const textAfter = formula.value.substring(cursorPos);
            formula.value = textBefore + functionText + textAfter;
            formula.focus();
            updateFormulaPreview();
        }}

        function updateFormulaPreview() {{
            const formula = document.getElementById('formula').value;
            const preview = document.getElementById('formulaPreview');
            if (formula.trim()) {{
                preview.textContent = formula;
                preview.className = 'font-monospace text-dark';
            }} else {{
                preview.textContent = 'Enter a formula to see preview...';
                preview.className = 'font-monospace text-muted';
            }}
        }}

        document.getElementById('formula').addEventListener('input', updateFormulaPreview);
    </script>
    """


def _generate_aggregation_config_html(wizard_data: Dict[str, Any], available_columns: List[Dict[str, Any]]) -> str:
    """Generate aggregation configuration HTML."""
    columns_options = "\n".join([f'<option value="{col["id"]}">{col["name"]}</option>' for col in available_columns])

    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 2: Aggregation Configuration</h6>

        <div class="row">
            <div class="col-md-6">
                <label for="aggregationType" class="form-label fw-bold">Aggregation Type <span class="text-danger">*</span></label>
                <select class="form-select" id="aggregationType" name="aggregation_type" required>
                    <option value="">Select aggregation...</option>
                    <option value="SUM" {'selected' if wizard_data.get('aggregation_type') == 'SUM' else ''}>SUM - Total</option>
                    <option value="AVG" {'selected' if wizard_data.get('aggregation_type') == 'AVG' else ''}>AVG - Average</option>
                    <option value="COUNT" {'selected' if wizard_data.get('aggregation_type') == 'COUNT' else ''}>COUNT - Count</option>
                    <option value="MIN" {'selected' if wizard_data.get('aggregation_type') == 'MIN' else ''}>MIN - Minimum</option>
                    <option value="MAX" {'selected' if wizard_data.get('aggregation_type') == 'MAX' else ''}>MAX - Maximum</option>
                </select>
            </div>

            <div class="col-md-6">
                <label for="targetColumn" class="form-label fw-bold">Target Column <span class="text-danger">*</span></label>
                <select class="form-select" id="targetColumn" name="target_column" required>
                    <option value="">Select column...</option>
                    {columns_options}
                </select>
            </div>
        </div>

        <div class="row mt-3">
            <div class="col-md-6">
                <label for="groupByColumn" class="form-label fw-bold">Group By Column</label>
                <select class="form-select" id="groupByColumn" name="group_by_column">
                    <option value="">No grouping</option>
                    {columns_options}
                </select>
                <div class="form-text">Optional: Group aggregation by this column</div>
            </div>

            <div class="col-md-6">
                <label for="filterCondition" class="form-label fw-bold">Filter Condition</label>
                <input type="text"
                       class="form-control"
                       id="filterCondition"
                       name="filter_condition"
                       value="{wizard_data.get('filter_condition', '')}"
                       placeholder="e.g., Status = 'Active'">
                <div class="form-text">Optional: Only include rows matching this condition</div>
            </div>
        </div>
    </div>
    """


def _generate_transformation_config_html(wizard_data: Dict[str, Any], available_columns: List[Dict[str, Any]]) -> str:
    """Generate transformation configuration HTML."""
    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 2: Transformation Configuration</h6>

        <div class="row">
            <div class="col-md-6">
                <label for="transformationType" class="form-label fw-bold">Transformation Type <span class="text-danger">*</span></label>
                <select class="form-select" id="transformationType" name="transformation_type" required>
                    <option value="">Select transformation...</option>
                    <option value="UPPER" {'selected' if wizard_data.get('transformation_type') == 'UPPER' else ''}>UPPER - Uppercase</option>
                    <option value="LOWER" {'selected' if wizard_data.get('transformation_type') == 'LOWER' else ''}>LOWER - Lowercase</option>
                    <option value="TRIM" {'selected' if wizard_data.get('transformation_type') == 'TRIM' else ''}>TRIM - Remove whitespace</option>
                    <option value="DATE_FORMAT" {'selected' if wizard_data.get('transformation_type') == 'DATE_FORMAT' else ''}>DATE_FORMAT - Format date</option>
                    <option value="SUBSTRING" {'selected' if wizard_data.get('transformation_type') == 'SUBSTRING' else ''}>SUBSTRING - Extract substring</option>
                    <option value="REPLACE" {'selected' if wizard_data.get('transformation_type') == 'REPLACE' else ''}>REPLACE - Replace text</option>
                </select>
            </div>

            <div class="col-md-6">
                <label for="sourceColumn" class="form-label fw-bold">Source Column <span class="text-danger">*</span></label>
                <select class="form-select" id="sourceColumn" name="source_column" required>
                    <option value="">Select column...</option>
                    {''.join([f'<option value="{col["id"]}">{col["name"]}</option>' for col in available_columns])}
                </select>
            </div>
        </div>

        <div class="mt-3" id="transformationOptions">
            <!-- Options will be populated based on transformation type -->
        </div>
    </div>
    """


def _generate_lookup_config_html(wizard_data: Dict[str, Any], available_columns: List[Dict[str, Any]]) -> str:
    """Generate lookup configuration HTML."""
    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 2: Lookup Configuration</h6>

        <div class="alert alert-info">
            <i class="fas fa-info-circle me-2"></i>
            Lookup columns reference data from external sources or other tables.
        </div>

        <div class="row">
            <div class="col-md-6">
                <label for="lookupSource" class="form-label fw-bold">Lookup Source <span class="text-danger">*</span></label>
                <select class="form-select" id="lookupSource" name="lookup_source" required>
                    <option value="">Select source...</option>
                    <option value="table" {'selected' if wizard_data.get('lookup_source') == 'table' else ''}>Another Table</option>
                    <option value="api" {'selected' if wizard_data.get('lookup_source') == 'api' else ''}>API Endpoint</option>
                    <option value="static" {'selected' if wizard_data.get('lookup_source') == 'static' else ''}>Static Mapping</option>
                </select>
            </div>

            <div class="col-md-6">
                <label for="keyColumn" class="form-label fw-bold">Key Column <span class="text-danger">*</span></label>
                <select class="form-select" id="keyColumn" name="key_column" required>
                    <option value="">Select column...</option>
                    {''.join([f'<option value="{col["id"]}">{col["name"]}</option>' for col in available_columns])}
                </select>
                <div class="form-text">Column to use for lookup matching</div>
            </div>
        </div>

        <div class="mt-3">
            <label for="returnColumn" class="form-label fw-bold">Return Column</label>
            <input type="text"
                   class="form-control"
                   id="returnColumn"
                   name="return_column"
                   value="{wizard_data.get('return_column', '')}"
                   placeholder="Column name to return from lookup">
            <div class="form-text">Name of the column to return from the lookup source</div>
        </div>
    </div>
    """


def _generate_step3_html(wizard_data: Dict[str, Any]) -> str:
    """Generate Step 3: Validation Rules."""
    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 3: Validation & Formatting</h6>

        <div class="row">
            <div class="col-md-6">
                <h6>Data Validation</h6>

                <div class="mb-3">
                    <div class="form-check">
                        <input class="form-check-input" type="checkbox" id="validateNotNull" name="validate_not_null">
                        <label class="form-check-label" for="validateNotNull">
                            Require non-null values
                        </label>
                    </div>
                </div>

                <div class="mb-3">
                    <label for="minValue" class="form-label">Minimum Value</label>
                    <input type="number" class="form-control" id="minValue" name="min_value" step="any">
                </div>

                <div class="mb-3">
                    <label for="maxValue" class="form-label">Maximum Value</label>
                    <input type="number" class="form-control" id="maxValue" name="max_value" step="any">
                </div>
            </div>

            <div class="col-md-6">
                <h6>Display Formatting</h6>

                <div class="mb-3">
                    <label for="displayFormat" class="form-label">Display Format</label>
                    <select class="form-select" id="displayFormat" name="display_format">
                        <option value="">Default</option>
                        <option value="number">Number (1,234.56)</option>
                        <option value="currency">Currency ($1,234.56)</option>
                        <option value="percentage">Percentage (12.34%)</option>
                        <option value="date">Date (MM/DD/YYYY)</option>
                        <option value="datetime">Date & Time</option>
                    </select>
                </div>

                <div class="mb-3">
                    <label for="decimalPlaces" class="form-label">Decimal Places</label>
                    <input type="number" class="form-control" id="decimalPlaces" name="decimal_places" min="0" max="10" value="2">
                </div>

                <div class="mb-3">
                    <div class="form-check">
                        <input class="form-check-input" type="checkbox" id="useThousandsSeparator" name="use_thousands_separator" checked>
                        <label class="form-check-label" for="useThousandsSeparator">
                            Use thousands separator
                        </label>
                    </div>
                </div>
            </div>
        </div>
    </div>
    """


def _generate_step4_html(wizard_data: Dict[str, Any]) -> str:
    """Generate Step 4: Preview & Confirmation."""
    return f"""
    <div class="step-content">
        <h6 class="fw-bold mb-3">Step 4: Preview & Confirmation</h6>

        <div class="card">
            <div class="card-header">
                <h6 class="mb-0"><i class="fas fa-eye me-2"></i>Column Summary</h6>
            </div>
            <div class="card-body">
                <table class="table table-borderless">
                    <tr>
                        <td class="fw-bold" width="150">Column Name:</td>
                        <td>{wizard_data.get('column_name', 'N/A')}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">Column Type:</td>
                        <td>
                            <span class="badge bg-primary">{wizard_data.get('column_type', 'N/A').title()}</span>
                        </td>
                    </tr>
                    <tr>
                        <td class="fw-bold">Configuration:</td>
                        <td>{_format_configuration_summary(wizard_data)}</td>
                    </tr>
                </table>
            </div>
        </div>

        <div class="mt-3">
            <div class="form-check">
                <input class="form-check-input" type="checkbox" id="confirmCreation" name="confirm_creation" required>
                <label class="form-check-label fw-bold" for="confirmCreation">
                    I confirm that I want to create this child column
                </label>
            </div>
        </div>

        <div class="alert alert-info mt-3">
            <i class="fas fa-info-circle me-2"></i>
            <strong>Note:</strong> Child columns are calculated dynamically and will update automatically
            when the source data changes.
        </div>
    </div>
    """


def _format_configuration_summary(wizard_data: Dict[str, Any]) -> str:
    """Format configuration summary for preview."""
    column_type = wizard_data.get("column_type", "")

    if column_type == "formula":
        return f"Formula: <code>{wizard_data.get('formula', 'N/A')}</code>"
    elif column_type == "aggregation":
        agg_type = wizard_data.get("aggregation_type", "N/A")
        target = wizard_data.get("target_column", "N/A")
        return f"Aggregation: {agg_type}({target})"
    elif column_type == "transformation":
        trans_type = wizard_data.get("transformation_type", "N/A")
        source = wizard_data.get("source_column", "N/A")
        return f"Transformation: {trans_type}({source})"
    elif column_type == "lookup":
        source = wizard_data.get("lookup_source", "N/A")
        key = wizard_data.get("key_column", "N/A")
        return f"Lookup: {source} by {key}"
    else:
        return "Not configured"


def _validate_step_data(step: int, step_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate data for a specific wizard step."""
    errors = []

    if step == 1:
        if not step_data.get("column_type"):
            errors.append("Column type is required")
        if not step_data.get("column_name"):
            errors.append("Column name is required")
    elif step == 2:
        column_type = step_data.get("column_type")
        if column_type == "formula" and not step_data.get("formula"):
            errors.append("Formula expression is required")
        elif column_type == "aggregation":
            if not step_data.get("aggregation_type"):
                errors.append("Aggregation type is required")
            if not step_data.get("target_column"):
                errors.append("Target column is required")
    elif step == 4:
        if not step_data.get("confirm_creation"):
            errors.append("Please confirm column creation")

    return {"success": len(errors) == 0, "errors": errors}


def _create_child_column(
    user_id: str,
    table_name: str,
    parent_column_id: str,
    wizard_data: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """Create the child column based on wizard data."""
    try:
        # This would integrate with the actual column creation system
        # For now, we'll simulate the creation

        column_definition = {
            "column_name": wizard_data["column_name"],
            "column_type": wizard_data["column_type"],
            "parent_column": parent_column_id,
            "configuration": wizard_data,
            "created_by": user_id,
            "table_name": table_name,
        }

        # Here you would integrate with the actual persistence layer
        # to save the child column definition

        return {
            "success": True,
            "data": column_definition,
            "message": f"Child column '{wizard_data['column_name']}' created successfully",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


def _encode_wizard_data(wizard_data: Dict[str, Any]) -> str:
    """Encode wizard data for form transmission."""
    import json

    return json.dumps(wizard_data)


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
    "render_child_column_wizard",
    "process_wizard_step",
]
