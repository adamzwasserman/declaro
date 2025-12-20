# Implementation Plan Guidelines for React2HTMX Modules

**Purpose**: Define how react2htmx creates bd issues from analysis results for systematic module implementation.

---

## bd Structure for Generated Modules

### Epic Structure

```
Epic: Implement [Module Name] from React2HTMX Analysis
├── Feature: Phase 1 - Domain Layer
│   ├── Task: Create BDD tests for [Model] model
│   ├── Task: Implement [Model] Pydantic model
│   └── Task: Create SQLAlchemy ORM model
├── Feature: Phase 2 - Service Layer
│   ├── Task: Create BDD tests for [service_name]
│   ├── Task: Implement [service_name] service
│   └── ... (for each service)
├── Feature: Phase 3 - Route Layer
│   ├── Task: Implement page routes
│   └── Task: Implement partial routes (HTMX)
├── Feature: Phase 4 - Template Layer
│   ├── Task: Create base template with HTMX
│   ├── Task: Create page templates
│   └── Task: Create partial templates
└── Feature: Phase 5 - Integration
    ├── Task: Wire up database connection
    ├── Task: Integration testing
    └── Task: Documentation
```

---

## Task Creation from Analysis Results

### From `SourceAnalysisResult.interfaces` → Domain Tasks

```bash
# For each interface found
bd create "Create BDD tests for [Interface] model" \
  -t task -p 1 \
  -d "1. Create feature file: tests/features/[name]_model.feature
      2. Create step definitions: tests/step_definitions/test_[name]_model_steps.py
      3. Test field validation and relationships
      4. Validate: uv run pytest tests/features/[name]_model.feature -v" \
  --deps parent-child:<phase1-id> --json

bd create "Implement [Interface] Pydantic model" \
  -t task -p 1 \
  -d "Fields: [list fields from interface]
      Location: domain/[name].py
      Include validators for: [required fields]" \
  --deps parent-child:<phase1-id>,blocks:<test-task-id> --json
```

### From `EnhancedFeature.services` → Service Tasks

```bash
# For each service
bd create "Create BDD tests for [service_name]" \
  -t task -p 1 \
  -d "Service: [service_name]([params]) -> [returns]
      Description: [description]
      Type: [Query|Mutation]
      Feature file: tests/features/[name]_service.feature
      Step definitions: tests/step_definitions/test_[name]_service_steps.py" \
  --deps parent-child:<phase2-id> --json

bd create "Implement [service_name] service" \
  -t task -p 1 \
  -d "Function signature:
      def [service_name](db: Session, [params]) -> [returns]:
          '''[description]'''
      Location: services/[name].py" \
  --deps parent-child:<phase2-id>,blocks:<test-task-id> --json
```

### From `EnhancedFeature.routes` → Route Tasks

```bash
# Page routes
bd create "Implement page routes" \
  -t task -p 1 \
  -d "Page routes to implement:
      [For each route where is_page=True:]
      - [METHOD] [path] -> [template]
        Services: [services]
      Location: routes/pages.py" \
  --deps parent-child:<phase3-id>,blocks:<phase2-id> --json

# Partial routes
bd create "Implement partial routes (HTMX)" \
  -t task -p 1 \
  -d "Partial routes for HTMX swaps:
      [For each route where is_page=False:]
      - [METHOD] [path] -> [template]
        Services: [services]
        HTMX: Returns fragment for hx-swap
      Location: routes/partials.py" \
  --deps parent-child:<phase3-id>,blocks:<phase2-id> --json
```

### From DOM Capture → Template Tasks

```bash
bd create "Create page templates" \
  -t task -p 1 \
  -d "Templates to create:
      [For each page template:]
      - templates/pages/[name].html
        Extends: base.html
        HTMX targets: [list target IDs]
        Semantic IDs to preserve: [list from capture]" \
  --deps parent-child:<phase4-id>,blocks:<phase3-id> --json

bd create "Create partial templates" \
  -t task -p 1 \
  -d "Partial templates for HTMX:
      [For each partial template:]
      - templates/partials/[name].html
        Purpose: [route description]
        No extends block - standalone fragment" \
  --deps parent-child:<phase4-id>,blocks:<phase3-id> --json
```

---

## Priority Assignment

| Source | Priority | Rationale |
|--------|----------|-----------|
| Domain models | 1 | Foundation for everything |
| Query services | 1 | Needed for page display |
| Page routes | 1 | Core functionality |
| Partial routes | 2 | HTMX enhancements |
| Mutation services | 2 | Write operations |
| Export services | 3 | Nice-to-have |

---

## Dependency Rules

1. **Tests before implementation**: Every implementation task depends on its test task
2. **Services before routes**: Routes depend on services being implemented
3. **Routes before templates**: Templates depend on routes for context
4. **Domain before services**: Services depend on models

```
Domain Tests → Domain Models
    ↓
Service Tests → Service Implementations
    ↓
Route Tests → Route Implementations
    ↓
Template Tests → Templates
```

---

## Task Description Quality

### Required Elements for BDD Tasks

```markdown
## Task: Create BDD tests for [feature]

### Technical Requirements:
- Framework: pytest-bdd
- Feature file: tests/features/[name].feature
- Step definitions: tests/step_definitions/test_[name]_steps.py

### Gherkin Scenarios (from analysis):
[Copy scenarios from EnhancedFeature.feature]

### Validation:
uv run pytest tests/features/[name].feature -v
Must show assertion failures, not StepDefinitionNotFound
```

### Required Elements for Implementation Tasks

```markdown
## Task: Implement [component]

### Specification:
- Location: [exact file path]
- Signature: [function/class signature]
- Dependencies: [imports needed]

### From Analysis:
[Reference to source analysis data]

### Acceptance Criteria:
- [ ] Tests pass: uv run pytest tests/features/[name].feature -v
- [ ] No type errors: uv run mypy [path]
- [ ] Code formatted: uv run ruff format [path]
```

---

## Automation: react2htmx bd Integration

react2htmx should:

1. **Create epic** for the module
2. **Create phase features** with proper dependencies
3. **Create tasks** from analysis results
4. **Set dependencies** following the rules above
5. **Output epic ID** for tracking

```bash
# react2htmx creates issues automatically
react2htmx convert URL --output DIR --create-bd-plan

# Output:
# Created epic: react2htmx-epic-45
# Created 5 phases, 23 tasks
# Run: bd ready --json to see available work
```

---

## Generated bd Commands

react2htmx outputs a shell script with all bd commands:

```bash
# output/implementation-plan.sh

#!/bin/bash
# Generated by react2htmx from [URL]
# Date: [timestamp]

# Create epic
EPIC_ID=$(bd create "Implement [Module] from React2HTMX" -t epic -p 1 \
  -d "Generated from [URL]. Features: [count], Services: [count], Routes: [count]" \
  --json | jq -r '.id')

# Phase 1: Domain
PHASE1_ID=$(bd create "Phase 1: Domain Layer" -t feature -p 1 \
  --deps parent-child:$EPIC_ID --json | jq -r '.id')

# ... tasks for phase 1 ...

# Phase 2: Services
PHASE2_ID=$(bd create "Phase 2: Service Layer" -t feature -p 1 \
  --deps parent-child:$EPIC_ID,blocks:$PHASE1_ID --json | jq -r '.id')

# ... etc ...

echo "Epic created: $EPIC_ID"
echo "Run: bd dep tree $EPIC_ID"
```
