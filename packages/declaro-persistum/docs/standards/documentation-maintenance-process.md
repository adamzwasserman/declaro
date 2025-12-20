# Documentation Maintenance Process

**Document Type**: Standards & Process
**Date**: 2025-12-13
**Version**: 1.0
**Status**: ACTIVE
**Owner**: Engineering Team

---

---
**IMPLEMENTATION STATUS**: PLANNED
**LAST VERIFIED**: 2025-12-13
**IMPLEMENTATION EVIDENCE**: Not yet implemented - new project
---

## Executive Summary

This document establishes a systematic process for maintaining the react2htmx documentation suite. Documentation must stay synchronized with implementation to remain useful.

### Core Principles

1. **Documentation is Code**: Docs live in repo, reviewed in PRs, versioned with git
2. **Update on Ship**: Features aren't "done" until docs are updated
3. **Quarterly Reviews**: Systematic audit every 3 months
4. **Automated Validation**: Pre-commit hooks enforce standards
5. **Single Source of Truth**: Implementation matrix tracks doc → code alignment

---

## Documentation Structure

### Directory Layout

```
docs/
├── architecture/
│   └── react2htmx-architecture.md     # Main architecture document
├── standards/
│   ├── python-coding-standards.md     # CLI tool development standards
│   ├── generated-code-standards.md    # Output quality standards
│   ├── performance-monitoring-standards.md
│   ├── documentation-maintenance-process.md
│   ├── implementation-plan-guidelines.md
│   └── architecture-doc-guidelines.md
├── implementation/
│   ├── implementation-matrix.md       # Doc → code tracking
│   └── [feature]-implementation.md    # Feature implementation docs
├── reviews/
│   └── YYYY-QN-documentation-review.md
└── templates/
    ├── status-header-template.md
    ├── architecture-doc-template.md
    └── implementation-doc-template.md
```

---

## Quarterly Documentation Review

### Schedule

Reviews occur on the **first Monday** of:
- **February** (Q1 review)
- **May** (Q2 review)
- **August** (Q3 review)
- **November** (Q4 review)

**Duration**: 2-3 hours
**Attendees**: Project maintainers

---

### Review Process

#### Step 1: Generate Documentation Health Report

```bash
./scripts/docs-health-check.sh
```

**Output**:
```
Documentation Health Report - 2025-12-13
=========================================

Total Documents: 12
With Status Headers: 12 (100%)
Without Status Headers: 0 (0%)

Implementation Status Distribution:
- IMPLEMENTED: 3 (25%)
- PLANNED: 6 (50%)
- IN PROGRESS: 3 (25%)

Last Verified Distribution:
- Within 30 days: 10 (83%)
- 30-90 days: 2 (17%)
- 90+ days: 0 (0%)

Documentation Health Score: 92/100
```

---

#### Step 2: Review Flagged Documents

Focus on documents flagged by health check:

1. **Last Verified > 90 days**: May be outdated
2. **Missing Evidence**: Claims implementation without proof
3. **Status = IN PROGRESS**: May have completed
4. **Status = PLANNED**: Check if implemented

**Process**:
1. Read document
2. Verify claims against codebase
3. Update status header
4. Add/update implementation evidence
5. Mark as verified (update LAST VERIFIED date)

---

#### Step 3: Update Implementation Matrix

The **Implementation Matrix** tracks doc → code alignment:

```markdown
# Implementation Matrix

| Doc | Title | Status | Implementation | % Complete | Last Verified |
|-----|-------|--------|----------------|------------|---------------|
| architecture | react2htmx Architecture | PLANNED | N/A | 0% | 2025-12-13 |
| python-coding | Python Coding Standards | PLANNED | N/A | 0% | 2025-12-13 |
| generated-code | Generated Code Standards | PLANNED | N/A | 0% | 2025-12-13 |
| performance | Performance Monitoring | PLANNED | N/A | 0% | 2025-12-13 |
```

**Location**: `docs/implementation/implementation-matrix.md`

---

#### Step 4: Generate Review Summary

```markdown
# Q1 2026 Documentation Review Summary

**Date**: 2026-02-03
**Duration**: 2.5 hours

## Health Score

**Previous Quarter**: N/A (new project)
**Current Quarter**: 92/100

## Actions Completed

1. Reviewed all 12 documents
2. Updated implementation evidence for completed features
3. Marked 2 documents as IN PROGRESS
4. Updated implementation matrix

## Issues Identified

1. **Capture module undocumented**: Implementation started, no docs
   - **Resolution**: Created capture implementation doc

## Action Items for Next Quarter

1. Document capture module implementation
2. Add API reference documentation
3. Create user guide

## Next Review

**Date**: May 5, 2026
```

**Location**: `docs/reviews/2026-Q1-documentation-review.md`

---

## Feature Shipment Documentation Process

### Requirement

**No feature ships without documentation.**

This means:
1. Architecture doc (if new feature) or update (if existing)
2. Implementation evidence in doc
3. Status header updated to IMPLEMENTED
4. Tests documented
5. CLI help text updated

---

### Process

#### Step 1: Create/Update Architecture Doc

**When**: During design phase, before implementation

```markdown
# [Feature Name]

**Date**: YYYY-MM-DD
**Status**: PLANNED
**Author**: [Your name]

---

---
**IMPLEMENTATION STATUS**: PLANNED
**LAST VERIFIED**: YYYY-MM-DD
**IMPLEMENTATION EVIDENCE**: Not yet implemented
---

## Overview

[What this feature does, why it exists]

## Technical Design

[Implementation approach]

## CLI Interface

[Command syntax, options]

## Testing Strategy

[How to test this feature]
```

---

#### Step 2: Update During Implementation

**When**: As implementation progresses

**Actions**:
1. Update status to IN PROGRESS
2. Add implementation evidence (file paths)
3. Document any deviations from plan
4. Update LAST VERIFIED date

---

#### Step 3: Finalize on Ship

**When**: Before merging to main

**Actions**:
1. Update status to IMPLEMENTED
2. Verify implementation evidence is accurate
3. Add usage examples
4. Document any known limitations
5. Update LAST VERIFIED date

**Pre-Merge Checklist**:
- [ ] Documentation exists/updated
- [ ] Status = IMPLEMENTED
- [ ] Implementation evidence accurate
- [ ] CLI help text updated
- [ ] Tests documented
- [ ] LAST VERIFIED = today

---

## Status Header Format

Every documentation file must include a status header:

```markdown
---
**IMPLEMENTATION STATUS**: [IMPLEMENTED | PLANNED | IN PROGRESS | SUPERSEDED | DEPRECATED]
**LAST VERIFIED**: YYYY-MM-DD
**IMPLEMENTATION EVIDENCE**: [File paths, or "Not yet implemented"]
---
```

### Status Definitions

| Status | Definition |
|--------|------------|
| IMPLEMENTED | Feature is fully implemented and shipped |
| PLANNED | Design complete, not yet started |
| IN PROGRESS | Implementation underway |
| SUPERSEDED | Replaced by newer document |
| DEPRECATED | No longer relevant |

---

## Pre-Commit Hook for Status Validation

### Hook Script

**Location**: `.git/hooks/pre-commit`

```bash
#!/bin/bash
# Documentation validation pre-commit hook

echo "Running documentation validation..."

# Find all markdown files in docs/
docs=$(find docs/ -name "*.md" -not -path "docs/reviews/*" -not -name "README.md")

errors=0

for doc in $docs; do
    # Check for status header
    if ! grep -q "^\*\*IMPLEMENTATION STATUS\*\*:" "$doc"; then
        echo "Missing status header: $doc"
        errors=$((errors + 1))
    fi

    # Check for last verified date
    if ! grep -q "^\*\*LAST VERIFIED\*\*:" "$doc"; then
        echo "Missing LAST VERIFIED: $doc"
        errors=$((errors + 1))
    fi

    # Extract status
    status=$(grep "^\*\*IMPLEMENTATION STATUS\*\*:" "$doc" | sed 's/.*: //')

    # Validate status value
    case "$status" in
        IMPLEMENTED|PLANNED|"IN PROGRESS"|SUPERSEDED|DEPRECATED)
            ;;
        *)
            echo "Invalid status '$status' in: $doc"
            errors=$((errors + 1))
            ;;
    esac

    # Check IMPLEMENTED docs have evidence
    if [[ "$status" == "IMPLEMENTED" ]]; then
        if ! grep -q "^\*\*IMPLEMENTATION EVIDENCE\*\*:" "$doc"; then
            echo "Missing evidence in IMPLEMENTED doc: $doc"
            errors=$((errors + 1))
        fi
    fi
done

if [[ $errors -gt 0 ]]; then
    echo ""
    echo "Documentation validation failed with $errors error(s)"
    exit 1
fi

echo "Documentation validation passed"
exit 0
```

**Installation**:
```bash
chmod +x .git/hooks/pre-commit
```

---

## Documentation Templates

### Architecture Document Template

```markdown
# [Feature Name]

**Date**: YYYY-MM-DD
**Version**: 1
**Status**: PLANNED
**Author**: [Name]

---

---
**IMPLEMENTATION STATUS**: PLANNED
**LAST VERIFIED**: YYYY-MM-DD
**IMPLEMENTATION EVIDENCE**: Not yet implemented
---

## Executive Summary

[2-3 paragraphs: What this feature does, why it exists]

## Problem Statement

[What problem does this solve?]

## Technical Design

### Architecture

[How it fits into the overall system]

### Implementation Approach

[Key technical decisions]

### Data Flow

[How data moves through the feature]

## CLI Interface

```
react2htmx [command] [options]

Options:
  --option    Description
```

## Testing Strategy

[Unit tests, integration tests, manual testing]

## Success Criteria

[How to verify the feature works]

## Open Questions

[Unresolved design decisions]
```

### Implementation Document Template

```markdown
# [Feature] Implementation

**Date**: YYYY-MM-DD
**Version**: 1
**Author**: [Name]

---

---
**IMPLEMENTATION STATUS**: IN PROGRESS
**LAST VERIFIED**: YYYY-MM-DD
**IMPLEMENTATION EVIDENCE**: [file paths]
---

## Overview

[Brief description of what was implemented]

## Implementation Details

### Files Changed

- `path/to/file.py` - [description]
- `path/to/other.py` - [description]

### Key Decisions

[Important implementation choices and why]

### Deviations from Design

[Any changes from the original architecture doc]

## Testing

### Test Coverage

- Unit tests: `tests/unit/test_feature.py`
- Integration tests: `tests/integration/test_feature.py`

### Manual Testing

[Steps to manually verify]

## Known Issues

[Any limitations or known bugs]

## Future Work

[Potential improvements, not in scope for this release]
```

---

## Automation Scripts

### Health Check Script

**Location**: `scripts/docs-health-check.sh`

```bash
#!/bin/bash
# Documentation health check script

echo "Documentation Health Report - $(date +%Y-%m-%d)"
echo "========================================="
echo ""

# Count total docs
total_docs=$(find docs/ -name "*.md" ! -name "README.md" ! -path "docs/reviews/*" | wc -l | tr -d ' ')
echo "Total Documents: $total_docs"

# Count docs with status headers
docs_with_headers=$(grep -l "^\*\*IMPLEMENTATION STATUS\*\*:" docs/**/*.md 2>/dev/null | wc -l | tr -d ' ')
echo "With Status Headers: $docs_with_headers"

# Status distribution
echo ""
echo "Implementation Status Distribution:"
for status in IMPLEMENTED PLANNED "IN PROGRESS" SUPERSEDED DEPRECATED; do
    count=$(grep -r "^\*\*IMPLEMENTATION STATUS\*\*: $status" docs/ 2>/dev/null | wc -l | tr -d ' ')
    if [ "$count" -gt 0 ]; then
        echo "- $status: $count"
    fi
done

echo ""
echo "Health check complete."
```

### Generate Implementation Matrix

**Location**: `scripts/generate-implementation-matrix.sh`

```bash
#!/bin/bash
# Generate implementation matrix from doc headers

echo "# Implementation Matrix"
echo ""
echo "**Last Updated**: $(date +%Y-%m-%d)"
echo ""
echo "| Doc | Title | Status | Evidence | Last Verified |"
echo "|-----|-------|--------|----------|---------------|"

find docs/ -name "*.md" ! -name "README.md" ! -path "docs/reviews/*" ! -path "docs/templates/*" | sort | while read doc; do
    doc_name=$(basename "$doc" .md)
    title=$(head -1 "$doc" | sed 's/# //')
    status=$(grep "^\*\*IMPLEMENTATION STATUS\*\*:" "$doc" | sed 's/.*: //' | head -1)
    evidence=$(grep "^\*\*IMPLEMENTATION EVIDENCE\*\*:" "$doc" | sed 's/.*: //' | head -1)
    verified=$(grep "^\*\*LAST VERIFIED\*\*:" "$doc" | sed 's/.*: //' | head -1)

    # Truncate evidence if too long
    if [ ${#evidence} -gt 30 ]; then
        evidence="${evidence:0:27}..."
    fi

    echo "| $doc_name | $title | $status | $evidence | $verified |"
done
```

---

## Escalation Path for Conflicts

### Conflict Types

1. **Doc says IMPLEMENTED, code doesn't exist**
   - **Resolution**: Update doc to PLANNED or remove evidence
   - **Timeline**: Same day

2. **Code exists, no doc**
   - **Resolution**: Create doc, mark IMPLEMENTED
   - **Timeline**: Within 1 week

3. **Doc and code disagree on design**
   - **Resolution**: Determine source of truth (usually code), update doc
   - **Timeline**: Within 3 days

---

## Success Metrics

### Documentation Quality Metrics

| Metric | Target | Notes |
|--------|--------|-------|
| Health Score | >= 90/100 | Calculated by health check |
| Docs with Headers | 100% | All docs have status headers |
| Verified < 30 days | >= 70% | Recently verified |
| Verified < 90 days | >= 95% | Not stale |
| Missing Evidence | 0% | IMPLEMENTED docs have proof |

---

## Conclusion

Documentation maintenance ensures react2htmx documentation remains accurate and useful. By following quarterly reviews, enforcing doc-on-ship policies, and using automated validation, we prevent documentation drift.

**Key Takeaways**:

1. **Documentation is Code**: Treat docs with same rigor as code
2. **No Ship Without Docs**: Features aren't done until documented
3. **Automate Validation**: Pre-commit hooks enforce standards
4. **Review Quarterly**: Systematic audits catch drift early
5. **Single Source of Truth**: Implementation matrix tracks alignment
