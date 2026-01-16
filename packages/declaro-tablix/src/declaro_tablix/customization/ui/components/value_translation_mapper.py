"""
Value translation mapper for TableV2 customization.

This module provides function-based HTMX component for mapping and translating
column values with visual interface and bulk operations.
"""

import json
from typing import Any, Dict, List, Optional

from declaro_advise import error, success, warning
from declaro_tablix.customization.ui import UI_CONFIG, UI_ERRORS, UI_SUCCESS


def render_value_translation_mapper(
    user_id: str,
    table_name: str,
    column_id: str,
    current_translations: Optional[Dict[str, str]] = None,
    sample_values: Optional[List[str]] = None,
    translation_mode: str = "manual",
    db_session=None,
) -> Dict[str, Any]:
    """
    Render the value translation mapper component.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        current_translations: Current translation mappings
        sample_values: Sample values from the column
        translation_mode: Mode of translation (manual, bulk, pattern)
        db_session: Optional database session

    Returns:
        Component render result with HTML and metadata
    """
    try:
        # Initialize default values
        if current_translations is None:
            current_translations = {}

        if sample_values is None:
            sample_values = []

        # Build component data
        component_data = {
            "user_id": user_id,
            "table_name": table_name,
            "column_id": column_id,
            "current_translations": current_translations,
            "sample_values": sample_values,
            "translation_mode": translation_mode,
            "available_modes": ["manual", "bulk", "pattern", "lookup"],
        }

        # Generate HTML content
        html_content = _generate_translation_mapper_html(component_data)

        return {
            "success": True,
            "html": html_content,
            "component_type": "value_translation_mapper",
            "metadata": component_data,
        }

    except Exception as e:
        error(f"Failed to render value translation mapper: {str(e)}")
        return {
            "success": False,
            "error": UI_ERRORS["render_error"],
            "html": _generate_error_html(str(e)),
        }


def process_translation_mapping(
    user_id: str,
    table_name: str,
    column_id: str,
    translations: Dict[str, str],
    translation_mode: str = "manual",
    db_session=None,
) -> Dict[str, Any]:
    """
    Process and save value translation mappings.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        translations: Translation mappings
        translation_mode: Mode used for translations
        db_session: Optional database session

    Returns:
        Processing result with success status
    """
    try:
        # Validate translations
        validation_result = _validate_translations(translations)
        if not validation_result["success"]:
            return {
                "success": False,
                "error": "Translation validation failed",
                "errors": validation_result["errors"],
            }

        # Save translations to customization persistence
        # This would integrate with the actual persistence layer
        customization_data = {
            "value_translations": translations,
            "translation_mode": translation_mode,
            "translation_count": len(translations),
        }

        # Here you would use the actual persistence functions
        # For now, we'll simulate the save operation

        success(UI_SUCCESS["customization_saved"])
        return {
            "success": True,
            "message": f"Saved {len(translations)} value translations",
            "data": customization_data,
            "refresh_component": True,
        }

    except Exception as e:
        error(f"Failed to process translation mapping: {str(e)}")
        return {"success": False, "error": str(e)}


def get_column_sample_values(
    table_name: str,
    column_id: str,
    limit: int = 50,
    db_session=None,
) -> Dict[str, Any]:
    """
    Get sample values from a column for translation mapping.

    Args:
        table_name: Name of the table
        column_id: Column identifier
        limit: Maximum number of sample values
        db_session: Optional database session

    Returns:
        Sample values with frequency counts
    """
    try:
        # This would integrate with the actual data access layer
        # For now, we'll simulate sample values
        sample_values = [
            {"value": "Y", "count": 150, "percentage": 45.5},
            {"value": "N", "count": 120, "percentage": 36.4},
            {"value": "MAYBE", "count": 60, "percentage": 18.2},
            {"value": "", "count": 0, "percentage": 0.0},
        ]

        return {
            "success": True,
            "data": sample_values,
            "total_rows": sum(v["count"] for v in sample_values),
            "distinct_values": len([v for v in sample_values if v["count"] > 0]),
        }

    except Exception as e:
        error(f"Failed to get sample values: {str(e)}")
        return {"success": False, "error": str(e)}


def _generate_translation_mapper_html(component_data: Dict[str, Any]) -> str:
    """Generate HTML for the value translation mapper."""
    user_id = component_data["user_id"]
    table_name = component_data["table_name"]
    column_id = component_data["column_id"]
    current_translations = component_data["current_translations"]
    sample_values = component_data["sample_values"]
    translation_mode = component_data["translation_mode"]
    available_modes = component_data["available_modes"]

    # Generate mode options
    mode_options = "\n".join(
        [
            f'<option value="{mode}" {"selected" if mode == translation_mode else ""}>{mode.title()}</option>'
            for mode in available_modes
        ]
    )

    # Generate sample values HTML
    sample_values_html = _generate_sample_values_html(sample_values, current_translations)

    # Generate current translations HTML
    current_translations_html = _generate_current_translations_html(current_translations)

    return f"""
    <div class="value-translation-mapper">
        <div class="row">
            <!-- Translation Mode Selection -->
            <div class="col-12 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-language me-2"></i>Value Translation Mapper</h6>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <label for="translationMode" class="form-label fw-bold">Translation Mode</label>
                                <select class="form-select"
                                        id="translationMode"
                                        name="translation_mode"
                                        onchange="changeTranslationMode(this.value)">
                                    {mode_options}
                                </select>
                            </div>
                            <div class="col-md-9">
                                <div class="mode-description">
                                    <div id="modeDescription">{_get_mode_description(translation_mode)}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Sample Values Panel -->
            <div class="col-md-6">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h6 class="mb-0"><i class="fas fa-chart-bar me-2"></i>Column Values</h6>
                        <button type="button"
                                class="btn btn-sm btn-outline-primary"
                                onclick="refreshSampleValues()">
                            <i class="fas fa-refresh me-1"></i>Refresh
                        </button>
                    </div>
                    <div class="card-body">
                        <div id="sampleValues" style="max-height: 400px; overflow-y: auto;">
                            {sample_values_html}
                        </div>

                        <div class="mt-3">
                            <button type="button"
                                    class="btn btn-outline-success btn-sm me-2"
                                    onclick="addAllToTranslations()">
                                <i class="fas fa-plus me-1"></i>Add All
                            </button>
                            <button type="button"
                                    class="btn btn-outline-info btn-sm"
                                    onclick="addSelectedToTranslations()">
                                <i class="fas fa-check me-1"></i>Add Selected
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Translation Mappings Panel -->
            <div class="col-md-6">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h6 class="mb-0"><i class="fas fa-exchange-alt me-2"></i>Translation Mappings</h6>
                        <div>
                            <button type="button"
                                    class="btn btn-sm btn-outline-danger me-2"
                                    onclick="clearAllTranslations()">
                                <i class="fas fa-trash me-1"></i>Clear All
                            </button>
                            <span class="badge bg-primary" id="translationCount">{len(current_translations)}</span>
                        </div>
                    </div>
                    <div class="card-body">
                        <div id="translationMappings" style="max-height: 350px; overflow-y: auto;">
                            {current_translations_html}
                        </div>

                        <!-- Add new translation -->
                        <div class="mt-3">
                            <div class="row g-2">
                                <div class="col-5">
                                    <input type="text"
                                           class="form-control form-control-sm"
                                           id="newOriginalValue"
                                           placeholder="Original value...">
                                </div>
                                <div class="col-5">
                                    <input type="text"
                                           class="form-control form-control-sm"
                                           id="newTranslatedValue"
                                           placeholder="Translated value...">
                                </div>
                                <div class="col-2">
                                    <button type="button"
                                            class="btn btn-primary btn-sm w-100"
                                            onclick="addNewTranslation()">
                                        <i class="fas fa-plus"></i>
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Bulk Operations (shown based on mode) -->
        <div class="row mt-3" id="bulkOperations" style="display: {'block' if translation_mode == 'bulk' else 'none'};">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-table me-2"></i>Bulk Translation Input</h6>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <label for="bulkInput" class="form-label fw-bold">Paste Mappings</label>
                                <textarea class="form-control font-monospace"
                                          id="bulkInput"
                                          rows="8"
                                          placeholder="Format: Original Value -> Translated Value
Y -> Yes
N -> No
MAYBE -> Perhaps"></textarea>
                                <div class="form-text">
                                    Enter one mapping per line using format: Original -> Translated
                                </div>
                            </div>
                            <div class="col-md-6">
                                <label for="csvInput" class="form-label fw-bold">CSV Upload</label>
                                <input type="file"
                                       class="form-control"
                                       id="csvInput"
                                       accept=".csv"
                                       onchange="processCsvFile(this)">
                                <div class="form-text">
                                    Upload CSV with columns: Original, Translated
                                </div>

                                <div class="mt-3">
                                    <button type="button"
                                            class="btn btn-success"
                                            onclick="processBulkInput()">
                                        <i class="fas fa-upload me-2"></i>Process Bulk Input
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Pattern Operations (shown based on mode) -->
        <div class="row mt-3" id="patternOperations" style="display: {'block' if translation_mode == 'pattern' else 'none'};">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-magic me-2"></i>Pattern-Based Translation</h6>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-4">
                                <label for="patternType" class="form-label fw-bold">Pattern Type</label>
                                <select class="form-select" id="patternType">
                                    <option value="case">Case Transformation</option>
                                    <option value="replace">Find & Replace</option>
                                    <option value="prefix">Add Prefix/Suffix</option>
                                    <option value="format">Format Pattern</option>
                                </select>
                            </div>
                            <div class="col-md-4">
                                <label for="patternRule" class="form-label fw-bold">Pattern Rule</label>
                                <input type="text"
                                       class="form-control"
                                       id="patternRule"
                                       placeholder="Enter pattern rule...">
                            </div>
                            <div class="col-md-4">
                                <div class="d-flex align-items-end h-100">
                                    <button type="button"
                                            class="btn btn-info w-100"
                                            onclick="applyPattern()">
                                        <i class="fas fa-magic me-2"></i>Apply Pattern
                                    </button>
                                </div>
                            </div>
                        </div>

                        <div class="mt-3">
                            <label class="form-label fw-bold">Pattern Preview</label>
                            <div class="card bg-light">
                                <div class="card-body">
                                    <div id="patternPreview" class="font-monospace text-muted">
                                        Select a pattern type and rule to see preview...
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Action Buttons -->
        <div class="row mt-3">
            <div class="col-12">
                <div class="d-flex justify-content-between">
                    <div>
                        <button type="button"
                                class="btn btn-outline-secondary"
                                onclick="previewTranslations()">
                            <i class="fas fa-eye me-2"></i>Preview Changes
                        </button>
                    </div>

                    <div>
                        <button type="button"
                                class="btn btn-secondary me-2"
                                onclick="resetTranslations()">
                            <i class="fas fa-undo me-2"></i>Reset
                        </button>

                        <button type="button"
                                class="btn btn-primary"
                                onclick="saveTranslations()"
                                id="saveTranslationsBtn">
                            <i class="fas fa-save me-2"></i>Save Translations
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Hidden form data -->
        <input type="hidden" name="user_id" value="{user_id}">
        <input type="hidden" name="table_name" value="{table_name}">
        <input type="hidden" name="column_id" value="{column_id}">
        <input type="hidden" name="translations_data" id="translationsData" value="">
    </div>

    <style>
        .value-item, .translation-item {{
            padding: 8px 12px;
            border: 1px solid #dee2e6;
            border-radius: 0.375rem;
            margin-bottom: 4px;
            background: white;
        }}

        .value-item:hover {{
            background: #f8f9fa;
            cursor: pointer;
        }}

        .value-item.selected {{
            background: #e3f2fd;
            border-color: #2196f3;
        }}

        .translation-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .translation-arrow {{
            color: #6c757d;
            margin: 0 8px;
        }}

        .value-count {{
            font-size: 0.75rem;
            background: #e9ecef;
            padding: 2px 6px;
            border-radius: 0.25rem;
        }}

        .mode-description {{
            padding: 8px 12px;
            background: #f8f9fa;
            border-radius: 0.375rem;
            font-size: 0.875rem;
        }}
    </style>

    <script>
        // Translation mapper JavaScript
        let currentTranslations = {json.dumps(current_translations)};
        let sampleValues = [];

        function changeTranslationMode(mode) {{
            // Show/hide relevant sections based on mode
            document.getElementById('bulkOperations').style.display = mode === 'bulk' ? 'block' : 'none';
            document.getElementById('patternOperations').style.display = mode === 'pattern' ? 'block' : 'none';

            // Update mode description
            const descriptions = {{
                'manual': 'Manually map individual values one by one with full control.',
                'bulk': 'Process multiple mappings at once using text input or CSV upload.',
                'pattern': 'Apply transformation patterns to automatically generate mappings.',
                'lookup': 'Use external lookup tables or APIs for value translation.'
            }};

            document.getElementById('modeDescription').textContent = descriptions[mode] || '';

            // Reload component with new mode
            htmx.ajax('POST', '/api/tableV2/customization/change-translation-mode', {{
                values: {{
                    user_id: '{user_id}',
                    table_name: '{table_name}',
                    column_id: '{column_id}',
                    translation_mode: mode
                }},
                target: '.value-translation-mapper'
            }});
        }}

        function refreshSampleValues() {{
            htmx.ajax('POST', '/api/tableV2/customization/get-sample-values', {{
                values: {{
                    table_name: '{table_name}',
                    column_id: '{column_id}'
                }},
                target: '#sampleValues'
            }});
        }}

        function selectValue(element, value) {{
            element.classList.toggle('selected');
        }}

        function addValueToTranslations(originalValue) {{
            const translatedValue = prompt(`Enter translation for "${{originalValue}}":`);
            if (translatedValue !== null && translatedValue.trim() !== '') {{
                currentTranslations[originalValue] = translatedValue.trim();
                updateTranslationDisplay();
            }}
        }}

        function addAllToTranslations() {{
            const valueElements = document.querySelectorAll('.value-item');
            valueElements.forEach(element => {{
                const value = element.dataset.value;
                if (!currentTranslations[value]) {{
                    const translatedValue = prompt(`Enter translation for "${{value}}":`);
                    if (translatedValue !== null && translatedValue.trim() !== '') {{
                        currentTranslations[value] = translatedValue.trim();
                    }}
                }}
            }});
            updateTranslationDisplay();
        }}

        function addSelectedToTranslations() {{
            const selectedElements = document.querySelectorAll('.value-item.selected');
            selectedElements.forEach(element => {{
                const value = element.dataset.value;
                if (!currentTranslations[value]) {{
                    const translatedValue = prompt(`Enter translation for "${{value}}":`);
                    if (translatedValue !== null && translatedValue.trim() !== '') {{
                        currentTranslations[value] = translatedValue.trim();
                    }}
                }}
            }});
            updateTranslationDisplay();
        }}

        function addNewTranslation() {{
            const originalValue = document.getElementById('newOriginalValue').value.trim();
            const translatedValue = document.getElementById('newTranslatedValue').value.trim();

            if (originalValue && translatedValue) {{
                currentTranslations[originalValue] = translatedValue;
                document.getElementById('newOriginalValue').value = '';
                document.getElementById('newTranslatedValue').value = '';
                updateTranslationDisplay();
            }} else {{
                alert('Please enter both original and translated values');
            }}
        }}

        function removeTranslation(originalValue) {{
            if (confirm(`Remove translation for "${{originalValue}}"?`)) {{
                delete currentTranslations[originalValue];
                updateTranslationDisplay();
            }}
        }}

        function clearAllTranslations() {{
            if (confirm('Remove all translations?')) {{
                currentTranslations = {{}};
                updateTranslationDisplay();
            }}
        }}

        function updateTranslationDisplay() {{
            const container = document.getElementById('translationMappings');
            const count = Object.keys(currentTranslations).length;

            document.getElementById('translationCount').textContent = count;

            if (count === 0) {{
                container.innerHTML = '<div class="text-muted text-center py-3">No translations defined</div>';
                return;
            }}

            const html = Object.entries(currentTranslations).map(([original, translated]) => {{
                return `
                    <div class="translation-item">
                        <div>
                            <code>${{original}}</code>
                            <span class="translation-arrow">→</span>
                            <strong>${{translated}}</strong>
                        </div>
                        <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeTranslation('${{original}}')">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                `;
            }}).join('');

            container.innerHTML = html;
        }}

        function processBulkInput() {{
            const bulkText = document.getElementById('bulkInput').value.trim();
            if (!bulkText) {{
                alert('Please enter bulk translation data');
                return;
            }}

            const lines = bulkText.split('\\n');
            let processed = 0;

            lines.forEach(line => {{
                const match = line.match(/^(.+?)\\s*->\\s*(.+)$/);
                if (match) {{
                    const [, original, translated] = match;
                    currentTranslations[original.trim()] = translated.trim();
                    processed++;
                }}
            }});

            if (processed > 0) {{
                updateTranslationDisplay();
                alert(`Processed ${{processed}} translations`);
                document.getElementById('bulkInput').value = '';
            }} else {{
                alert('No valid translations found. Use format: Original -> Translated');
            }}
        }}

        function processCsvFile(input) {{
            const file = input.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = function(e) {{
                const csv = e.target.result;
                const lines = csv.split('\\n');
                let processed = 0;

                lines.forEach((line, index) => {{
                    if (index === 0) return; // Skip header
                    const [original, translated] = line.split(',').map(s => s.trim().replace(/^"|"$/g, ''));
                    if (original && translated) {{
                        currentTranslations[original] = translated;
                        processed++;
                    }}
                }});

                if (processed > 0) {{
                    updateTranslationDisplay();
                    alert(`Processed ${{processed}} translations from CSV`);
                }}
            }};
            reader.readAsText(file);
        }}

        function applyPattern() {{
            const patternType = document.getElementById('patternType').value;
            const patternRule = document.getElementById('patternRule').value.trim();

            if (!patternRule) {{
                alert('Please enter a pattern rule');
                return;
            }}

            // Apply pattern logic based on type
            const valueElements = document.querySelectorAll('.value-item');
            let processed = 0;

            valueElements.forEach(element => {{
                const originalValue = element.dataset.value;
                let translatedValue = originalValue;

                switch (patternType) {{
                    case 'case':
                        if (patternRule === 'upper') {{
                            translatedValue = originalValue.toUpperCase();
                        }} else if (patternRule === 'lower') {{
                            translatedValue = originalValue.toLowerCase();
                        }} else if (patternRule === 'title') {{
                            translatedValue = originalValue.replace(/\\w\\S*/g, (txt) =>
                                txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase());
                        }}
                        break;
                    case 'replace':
                        const [find, replace] = patternRule.split('->').map(s => s.trim());
                        if (find && replace !== undefined) {{
                            translatedValue = originalValue.replace(new RegExp(find, 'g'), replace);
                        }}
                        break;
                    case 'prefix':
                        if (patternRule.startsWith('+')) {{
                            translatedValue = patternRule.substring(1) + originalValue;
                        }} else if (patternRule.endsWith('+')) {{
                            translatedValue = originalValue + patternRule.slice(0, -1);
                        }}
                        break;
                }}

                if (translatedValue !== originalValue) {{
                    currentTranslations[originalValue] = translatedValue;
                    processed++;
                }}
            }});

            if (processed > 0) {{
                updateTranslationDisplay();
                alert(`Applied pattern to ${{processed}} values`);
            }} else {{
                alert('Pattern did not match any values');
            }}
        }}

        function previewTranslations() {{
            const count = Object.keys(currentTranslations).length;
            if (count === 0) {{
                alert('No translations to preview');
                return;
            }}

            const preview = Object.entries(currentTranslations)
                .map(([original, translated]) => `${{original}} → ${{translated}}`)
                .join('\\n');

            alert(`Preview of ${{count}} translations:\\n\\n${{preview}}`);
        }}

        function resetTranslations() {{
            if (confirm('Reset all translations to their original state?')) {{
                location.reload();
            }}
        }}

        function saveTranslations() {{
            const translationsData = JSON.stringify(currentTranslations);
            document.getElementById('translationsData').value = translationsData;

            htmx.ajax('POST', '/api/tableV2/customization/save-translations', {{
                values: {{
                    user_id: '{user_id}',
                    table_name: '{table_name}',
                    column_id: '{column_id}',
                    translations: translationsData,
                    translation_mode: document.getElementById('translationMode').value
                }},
                target: '.value-translation-mapper'
            }});
        }}

        // Handle Enter key in new translation inputs
        document.getElementById('newOriginalValue').addEventListener('keypress', function(e) {{
            if (e.key === 'Enter') {{
                document.getElementById('newTranslatedValue').focus();
            }}
        }});

        document.getElementById('newTranslatedValue').addEventListener('keypress', function(e) {{
            if (e.key === 'Enter') {{
                addNewTranslation();
            }}
        }});
    </script>
    """


def _generate_sample_values_html(sample_values: List[str], current_translations: Dict[str, str]) -> str:
    """Generate HTML for sample values display."""
    if not sample_values:
        return '<div class="text-muted text-center py-3">No sample values available</div>'

    html_items = []
    for value in sample_values:
        is_translated = value in current_translations
        status_icon = (
            '<i class="fas fa-check text-success"></i>' if is_translated else '<i class="fas fa-plus text-muted"></i>'
        )
        translated_text = f" → {current_translations[value]}" if is_translated else ""

        html_items.append(
            f"""
        <div class="value-item" data-value="{value}" onclick="selectValue(this, '{value}')">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    {status_icon}
                    <code class="ms-2">{value}</code>
                    <small class="text-muted">{translated_text}</small>
                </div>
                <button type="button"
                        class="btn btn-sm btn-outline-primary"
                        onclick="addValueToTranslations('{value}')">
                    <i class="fas fa-edit"></i>
                </button>
            </div>
        </div>
        """
        )

    return "\n".join(html_items)


def _generate_current_translations_html(current_translations: Dict[str, str]) -> str:
    """Generate HTML for current translations display."""
    if not current_translations:
        return '<div class="text-muted text-center py-3">No translations defined</div>'

    html_items = []
    for original, translated in current_translations.items():
        html_items.append(
            f"""
        <div class="translation-item">
            <div>
                <code>{original}</code>
                <span class="translation-arrow">→</span>
                <strong>{translated}</strong>
            </div>
            <button type="button"
                    class="btn btn-sm btn-outline-danger"
                    onclick="removeTranslation('{original}')">
                <i class="fas fa-times"></i>
            </button>
        </div>
        """
        )

    return "\n".join(html_items)


def _get_mode_description(mode: str) -> str:
    """Get description for translation mode."""
    descriptions = {
        "manual": "Manually map individual values one by one with full control.",
        "bulk": "Process multiple mappings at once using text input or CSV upload.",
        "pattern": "Apply transformation patterns to automatically generate mappings.",
        "lookup": "Use external lookup tables or APIs for value translation.",
    }
    return descriptions.get(mode, "")


def _validate_translations(translations: Dict[str, str]) -> Dict[str, Any]:
    """Validate translation mappings."""
    errors = []

    if not translations:
        errors.append("No translations provided")

    # Check for empty values
    for original, translated in translations.items():
        if not original or not original.strip():
            errors.append("Original values cannot be empty")
        if not translated or not translated.strip():
            errors.append(f"Translation for '{original}' cannot be empty")

    # Check for circular mappings
    for original, translated in translations.items():
        if translated in translations and translations[translated] == original:
            errors.append(f"Circular mapping detected: {original} ↔ {translated}")

    return {"success": len(errors) == 0, "errors": errors}


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
    "render_value_translation_mapper",
    "process_translation_mapping",
    "get_column_sample_values",
]
