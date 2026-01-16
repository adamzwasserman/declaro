# TableV2 Customization System User Guide

## Overview

The TableV2 customization system provides powerful tools for personalizing table views while maintaining data integrity and performance. This guide covers all customization features available to users and administrators.

## Table of Contents

1. [Getting Started](#getting-started)
2. [User Customizations](#user-customizations)
3. [Admin System Configuration](#admin-system-configuration)
4. [Formula Columns](#formula-columns)
5. [Value Translations](#value-translations)
6. [Performance Considerations](#performance-considerations)
7. [Security Features](#security-features)
8. [Troubleshooting](#troubleshooting)

## Getting Started

### Prerequisites

- Access to TableV2 system
- Valid user account with appropriate permissions
- Basic understanding of spreadsheet concepts

### Core Concepts

- **Base Data**: Original table data shared across all users
- **Customizations**: Personal modifications that don't affect others
- **System Aliases**: Admin-configured column names for consistency
- **Formula Columns**: Calculated columns using business logic
- **Value Translations**: Converting raw values to business-friendly labels

## User Customizations

### Column Renaming

Users can rename columns to match their personal workflow:

```python
# Example: Rename columns for better understanding
user_customization.rename_column("revenue_usd", "Sales Revenue")
user_customization.rename_column("cost_usd", "Total Costs")
```

**Features:**
- Personal column names don't affect other users
- Original data remains unchanged
- Renames persist across sessions
- Compatible with all other customizations

### Personal Formula Columns

Create custom calculations based on existing data:

```python
# Example: Add profit margin calculation
user_customization.add_formula_column(
    "profit_margin",
    "({revenue} - {cost}) / {revenue} * 100"
)
```

**Supported Functions:**
- Basic arithmetic: `+`, `-`, `*`, `/`
- Mathematical functions: `round()`, `abs()`, `max()`, `min()`
- Comparison operators: `>`, `<`, `>=`, `<=`, `==`
- Conditional logic: `if-then-else` expressions

### Value Translations

Convert raw data values to meaningful labels:

```python
# Example: Translate status codes
user_customization.set_value_translations("status", {
    "A": "Active",
    "I": "Inactive",
    "P": "Pending"
})
```

**Use Cases:**
- Status codes to readable labels
- Category codes to descriptions
- Numeric codes to business terms
- Abbreviations to full names

## Admin System Configuration

### System-Wide Aliases

Administrators can set consistent column names for all users:

```python
admin_config = AdminConfiguration(
    user_id="admin_001",
    table_name="financial_data",
    system_aliases={
        "revenue_usd": "Total Revenue",
        "cost_usd": "Total Cost",
        "profit_usd": "Net Profit"
    }
)
```

**Benefits:**
- Consistent terminology across organization
- Improved data comprehension
- Reduced confusion for new users
- Maintains data integrity

### Admin Formula Columns

Create system-wide calculated columns:

```python
admin_config.formula_columns = {
    "profit_margin": "{profit} / {revenue} * 100",
    "cost_ratio": "{cost} / {revenue} * 100"
}
```

**Best Practices:**
- Use for common business calculations
- Validate formulas thoroughly before deployment
- Document formula logic for users
- Consider performance impact on large datasets

## Formula Columns

### Syntax Guide

Formula columns use a simple, Excel-like syntax:

#### Basic Arithmetic
```
{column_name} + {other_column}
{price} * {quantity}
{revenue} - {cost}
{total} / {count}
```

#### Mathematical Functions
```
round({value}, 2)           # Round to 2 decimal places
abs({difference})           # Absolute value
max({value1}, {value2})     # Maximum of two values
min({score1}, {score2})     # Minimum of two values
```

#### Conditional Logic
```
{score} > 80 ? "Pass" : "Fail"
{status} == "A" ? "Active" : "Inactive"
```

### Advanced Examples

#### Business Calculations
```python
# Revenue per unit
"revenue_per_unit": "{total_revenue} / {units_sold}"

# Growth rate
"growth_rate": "({current_value} - {previous_value}) / {previous_value} * 100"

# Profit margin
"profit_margin": "({revenue} - {cost}) / {revenue} * 100"
```

#### Data Quality Checks
```python
# Flag missing data
"has_complete_data": "{field1} != '' and {field2} != '' and {field3} != ''"

# Outlier detection
"is_outlier": "{value} > {average} * 2 or {value} < {average} * 0.5"
```

### Security and Validation

All formulas undergo security validation:

- **Syntax checking**: Validates formula structure
- **Security scanning**: Prevents malicious code injection
- **Dependency analysis**: Detects circular references
- **Performance analysis**: Estimates execution cost

## Value Translations

### Setting Up Translations

```python
# Single column translation
user_customization.set_value_translations("priority", {
    "1": "High",
    "2": "Medium",
    "3": "Low"
})

# Multiple columns
translations = {
    "status": {"A": "Active", "I": "Inactive"},
    "region": {"N": "North", "S": "South", "E": "East", "W": "West"}
}

for column, translation_map in translations.items():
    user_customization.set_value_translations(column, translation_map)
```

### Translation Strategies

#### Code to Description
```python
# Product codes to names
"product_code": {
    "WGT001": "Premium Widget",
    "WGT002": "Standard Widget",
    "WGT003": "Economy Widget"
}
```

#### Status Indicators
```python
# Numeric status to labels
"order_status": {
    "0": "Pending",
    "1": "Processing",
    "2": "Shipped",
    "3": "Delivered",
    "4": "Cancelled"
}
```

## Performance Considerations

### Optimization Guidelines

1. **Formula Complexity**: Keep formulas simple for better performance
2. **Caching**: System automatically caches frequent calculations
3. **Batch Operations**: Use batch processing for large datasets
4. **Memory Management**: System uses hybrid architecture for efficiency

### Performance Targets

- **Individual Operations**: < 5ms per formula evaluation
- **Batch Processing**: < 10ms per 1000 rows
- **Memory Usage**: Shared base data, isolated customizations
- **Concurrent Users**: Supports 100+ simultaneous users

### Monitoring

The system provides performance metrics:

- Formula execution times
- Cache hit rates
- Memory usage patterns
- User activity metrics

## Security Features

### Formula Security

All formulas undergo comprehensive security validation:

```python
# Security levels
SecurityLevel.LOW     # Basic validation
SecurityLevel.MEDIUM  # Standard security (default)
SecurityLevel.HIGH    # Maximum security
```

### Protection Features

- **Code Injection Prevention**: Blocks malicious code
- **Resource Limits**: Prevents excessive memory/CPU usage
- **Audit Logging**: Tracks all security events
- **Access Control**: User-based permissions

### Safe Functions

Only approved functions are allowed in formulas:

**Mathematical**: `abs`, `round`, `max`, `min`, `sum`, `len`
**Type Conversion**: `int`, `float`, `str`
**Comparison**: `>`, `<`, `>=`, `<=`, `==`, `!=`

## Troubleshooting

### Common Issues

#### Formula Compilation Errors

**Error**: "Invalid column reference"
**Solution**: Verify column names exist and use correct syntax `{column_name}`

**Error**: "Circular dependency detected"
**Solution**: Remove circular references between formula columns

**Error**: "Function not allowed"
**Solution**: Use only approved functions listed in security section

#### Performance Issues

**Issue**: Slow formula execution
**Solutions**:
- Simplify complex formulas
- Use batch operations for large datasets
- Check for circular dependencies
- Review formula complexity

#### Data Display Problems

**Issue**: Translations not appearing
**Solutions**:
- Verify translation mappings
- Check for exact value matches
- Ensure translations are saved properly

### Getting Help

1. **Check Error Messages**: Detailed error messages provide specific guidance
2. **Review Formula Syntax**: Ensure proper column reference format
3. **Test with Simple Data**: Start with basic examples
4. **Contact Support**: For persistent issues, contact system administrators

### Best Practices

1. **Start Simple**: Begin with basic customizations
2. **Test Thoroughly**: Validate formulas with sample data
3. **Document Changes**: Keep track of customizations
4. **Performance First**: Consider impact on system performance
5. **Security Aware**: Follow security guidelines for formulas

## API Reference

### User Customization Methods

```python
# Column renaming
user_customization.rename_column(original_name, new_name)

# Formula columns
user_customization.add_formula_column(column_name, formula)

# Value translations
user_customization.set_value_translations(column_name, translation_map)

# View data
user_customization.get_visible_columns(data)
```

### Admin Configuration Methods

```python
# System aliases
admin_config.system_aliases = {original: alias, ...}

# Admin formula columns
admin_config.formula_columns = {name: formula, ...}

# Configuration validation
admin_config.validate()
```

## Integration Examples

### Business Reporting Workflow

```python
# 1. Set up business-friendly column names
user_customization.rename_column("cust_id", "Customer ID")
user_customization.rename_column("ord_amt", "Order Amount")
user_customization.rename_column("ord_date", "Order Date")

# 2. Add calculated business metrics
user_customization.add_formula_column(
    "revenue_category",
    "round({ord_amt} / 1000, 0)"
)

user_customization.add_formula_column(
    "is_high_value",
    "{ord_amt} > 1000"
)

# 3. Apply value translations
user_customization.set_value_translations("customer_tier", {
    "A": "Premium",
    "B": "Standard",
    "C": "Basic"
})

# 4. Generate report with customized view
report_data = apply_customizations(raw_data, user_context)
```

### Data Quality Dashboard

```python
# Create data quality indicators
formulas = {
    "completeness_score": "({field1} != '' and {field2} != '' and {field3} != '') ? 100 : 0",
    "is_valid_email": "{email} contains '@' and {email} contains '.'",
    "date_range_valid": "{start_date} <= {end_date}",
    "numeric_range_check": "{value} >= 0 and {value} <= 100"
}

for name, formula in formulas.items():
    user_customization.add_formula_column(name, formula)
```

## Version History

- **v1.0**: Initial release with basic customization features
- **v1.1**: Added formula columns and value translations
- **v1.2**: Enhanced security and performance monitoring
- **v1.3**: Hybrid memory architecture implementation
- **v1.4**: User acceptance testing and integration improvements

## Support and Resources

- **Documentation**: Complete API reference and examples
- **Performance Monitoring**: Real-time system metrics
- **Security Auditing**: Comprehensive security event logging
- **User Training**: Interactive tutorials and examples
- **Technical Support**: Contact system administrators for assistance

---

*This user guide is part of the TableV2 customization system. For technical implementation details, see the developer documentation.*
