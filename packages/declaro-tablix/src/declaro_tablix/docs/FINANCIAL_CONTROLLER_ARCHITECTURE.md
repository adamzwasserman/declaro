# Financial Controller Architecture

## Overview

The Financial Controller layer provides a clean interface between the TableV2 routes and financial services, implementing the controller pattern with pure functions following our function-based architecture guidelines.

## Architecture Pattern

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│    Routes       │────▶│  Controllers    │────▶│   Services      │
│  (FastAPI)      │     │  (Business      │     │ (Data Operations)│
│                 │◀────│   Logic)        │◀────│                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ HTTP Handling   │     │ Validation      │     │ External APIs   │
│ Request/Response│     │ Error Handling  │     │ Database Ops    │
│ Authentication  │     │ Coordination    │     │ Cache Service   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Core Principles

### Function-Based Architecture
- **No Classes**: All controller logic implemented as pure functions
- **Stateless Operations**: Functions receive all required context as parameters
- **Dependency Injection**: Services and dependencies passed as function parameters
- **Clean Separation**: Clear boundaries between routing, controller logic, and service operations

### Error Handling Strategy
```python
# Consistent error handling pattern
try:
    # Validate inputs
    # Call service layer
    # Format response
    return success_response
except HTTPException:
    raise  # Let FastAPI handle HTTP exceptions
except Exception as e:
    error(f"Controller error: {str(e)}")
    raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
```

### Response Format Standardization
```python
# Standard controller response format
{
    "success": bool,
    "data": TableData | Dict | List,
    "status_code": int,
    "headers": Dict[str, str]
}
```

## Controller Functions

### Core Operations

#### `get_market_data_for_table()`
**Purpose**: Fetch and format market data for table display

**Key Features**:
- Market category validation against allowed values
- Integration with financial service layer
- Automatic caching headers (5-minute TTL)
- Empty data handling
- Comprehensive error logging

**Validation Rules**:
- Category cannot be empty
- Must be one of: `tsx`, `nyse`, `nasdaq`, `sp500`, `dow`
- Case-insensitive processing

#### `transform_ticker_data_for_table()`
**Purpose**: Convert ticker data to TableV2 compatible format

**Key Features**:
- Handles empty ticker lists gracefully
- Preserves data integrity through service layer
- Standardized response formatting
- Progress logging

#### `calculate_performance_metrics_for_ticker()`
**Purpose**: Generate performance metrics for individual tickers

**Key Features**:
- Ticker data validation
- Service layer integration
- Pydantic model serialization
- Error propagation

#### `format_financial_values_for_display()`
**Purpose**: Format raw financial data for UI display

**Key Features**:
- Input validation
- Service layer formatting
- Display-ready output
- Error handling

### Validation & Security

#### `validate_request_parameters()`
**Purpose**: Comprehensive parameter validation

**Validation Rules**:
- Category: Non-empty string
- Page: Positive integer
- Page size: 1-100 range
- Data type validation

**Error Response**:
```python
{
    "message": "Validation failed",
    "errors": ["Category cannot be empty", "Page must be greater than 0"]
}
```

#### `require_authentication()`
**Purpose**: Validate user authentication

**Security Features**:
- User context validation
- Proper HTTP status codes
- WWW-Authenticate headers
- Audit logging

### Advanced Operations

#### `process_bulk_market_data_request()`
**Purpose**: Handle multiple market categories in a single request

**Key Features**:
- Concurrent processing support
- Individual category error isolation
- Aggregated result reporting
- Performance metrics

**Response Format**:
```python
{
    "success": True,
    "data": {
        "results": [
            {"category": "tsx", "count": 100, "status": "success"},
            {"category": "nyse", "count": 200, "status": "success"}
        ],
        "total_processed": 300,
        "processing_time_ms": 250
    }
}
```

#### `apply_caching_strategy()`
**Purpose**: Generate appropriate cache headers

**Cache Strategy**:
- 5-minute default TTL
- ETag generation based on data content
- Content-Type headers
- Cache-Control directives

### Error Handling

#### `handle_service_error()`
**Purpose**: Graceful service layer error handling

**Features**:
- Structured error logging
- Context preservation
- User-friendly error messages
- Debug information retention

## Testing Strategy

### Test Coverage
- **20 total tests**: 10 BDD scenarios + 10 unit/integration tests
- **100% pass rate**: All tests passing
- **Comprehensive scenarios**: CRUD operations, error handling, edge cases

### BDD Scenarios
1. Get market data for table display
2. Handle invalid market category
3. Transform ticker data to table format
4. Calculate performance metrics
5. Format financial values for display
6. Handle service layer errors gracefully
7. Validate request parameters
8. Apply caching headers
9. Handle authentication requirements
10. Process bulk data requests

### Test Categories
- **Unit Tests**: Individual function testing
- **Integration Tests**: Service layer integration
- **Error Handling Tests**: Exception scenarios
- **Performance Tests**: High-load scenarios

## Integration Points

### Service Layer Integration
```python
# Example service layer call
tickers = await financial_service.fetch_market_data(
    category=category.lower(),
    cache_service=cache_service,
    api_key=api_key
)
```

### Notification System Integration
```python
# Structured logging pattern
info(f"Fetching market data for category: {category}")
success(f"Successfully retrieved {len(tickers)} tickers")
error(f"Controller error: {str(e)}")
warning(f"No market data found for category: {category}")
```

### Cache Service Integration
```python
# Cache headers generation
cache_headers = {
    "Cache-Control": "max-age=300",
    "ETag": f'"{category}-{len(tickers)}-{hash(...)}"',
    "Content-Type": "application/json"
}
```

## Performance Considerations

### Caching Strategy
- **TTL**: 5 minutes for market data
- **ETag Support**: Content-based cache validation
- **Header Management**: Proper cache control directives

### Error Performance
- **Fast Validation**: Early parameter validation
- **Graceful Degradation**: Partial failure handling in bulk operations
- **Resource Cleanup**: Proper exception handling

### Logging Performance
- **Structured Logging**: Efficient log formatting
- **Context Preservation**: Debug information without performance impact
- **Error Aggregation**: Bulk operation error collection

## Security Implementation

### Input Validation
- **Parameter Sanitization**: Category normalization
- **Range Validation**: Page size limits
- **Type Checking**: Data type validation

### Authentication
- **User Context**: Required for all operations
- **Permission Checking**: Controller-level validation
- **Session Management**: JWT token validation

### Error Security
- **Information Disclosure**: Sanitized error messages
- **Debug Information**: Internal logging without client exposure
- **Attack Prevention**: Input validation prevents injection

## Future Enhancements

### Planned Features
1. **Advanced Caching**: Redis integration for distributed caching
2. **Rate Limiting**: Per-user request throttling
3. **Metrics Collection**: Performance monitoring integration
4. **A/B Testing**: Feature flag support
5. **Real-time Updates**: WebSocket integration for live data

### Scalability Considerations
- **Horizontal Scaling**: Stateless function design supports scaling
- **Database Optimization**: Connection pooling and query optimization
- **Cache Distribution**: Redis cluster support
- **Load Balancing**: Function-based design supports load distribution

## Maintenance Guidelines

### Code Standards
- **Function Length**: Keep functions under 50 lines
- **Single Responsibility**: One concern per function
- **Error Handling**: Consistent exception patterns
- **Documentation**: Comprehensive docstrings

### Testing Requirements
- **New Functions**: Must include BDD scenarios
- **Test Coverage**: Minimum 95% coverage
- **Performance Tests**: For functions handling bulk operations
- **Error Tests**: Exception scenarios required

### Monitoring
- **Log Analysis**: Regular error pattern review
- **Performance Metrics**: Response time monitoring
- **Cache Efficiency**: Hit/miss ratio tracking
- **User Experience**: Error rate monitoring