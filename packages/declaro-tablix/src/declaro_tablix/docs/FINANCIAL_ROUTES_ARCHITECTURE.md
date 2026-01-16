# Financial Routes Architecture

## Overview
This document describes the route layer architecture for the TableV2 financial data API, implementing clean architecture patterns with function-based design.

## Architecture Principles
- **Clean Architecture**: Routes → Controllers → Services → Domain
- **Function-Based Design**: No classes except Pydantic models, SQLAlchemy models, global registries, and API exceptions
- **Separation of Concerns**: Clear boundaries between route handling, business logic, and data operations
- **Dependency Injection**: FastAPI dependencies for user context, cache service, and other external services

## Route Layer Structure

### Core Components

#### 1. Route Handlers (`financial_routes.py`)
Six main endpoint categories:

**Market Data Endpoints:**
- `GET /api/v1/financial/market-data/{category}` - Retrieve market data with pagination
- `GET /api/v1/financial/metrics/{symbol}` - Get performance metrics for a ticker
- `GET /api/v1/financial/health` - Service health monitoring

**Data Processing Endpoints:**
- `POST /api/v1/financial/transform` - Transform ticker data to TableV2 format
- `POST /api/v1/financial/format` - Format financial values for display
- `POST /api/v1/financial/bulk` - Process bulk market data requests

#### 2. Request/Response Models (`financial_models.py`)
Comprehensive Pydantic models with validation:

**Request Models:**
- `MarketDataRequest` - Market category validation and pagination parameters
- `TransformRequest` - Ticker data transformation with table configuration
- `MetricsRequest` - Performance metrics calculation parameters
- `FormatRequest` - Financial value formatting options
- `BulkRequest` - Multi-category bulk processing configuration

**Response Models:**
- `MarketDataResponse` - Structured market data with pagination metadata
- `TransformResponse` - Transformed table data with conversion information
- `MetricsResponse` - Performance metrics with calculation metadata
- `FormatResponse` - Formatted financial values with locale information
- `BulkResponse` - Aggregated bulk processing results
- `HealthResponse` - Service health status and performance metrics

### Design Patterns

#### 1. Function-Based Route Handlers
```python
@router.get("/market-data/{category}", response_model=MarketDataResponse)
async def get_market_data(
    category: str,
    request: Request,
    page: int = Query(default=1, ge=1, description="Page number"),
    user: Dict = Depends(get_current_user),
    cache_service: Any = Depends(get_cache_service)
) -> MarketDataResponse:
    """Pure function route handler with dependency injection."""
```

#### 2. Controller Integration
```python
# Call controller layer with pure function interface
result = await financial_controller.get_market_data_for_table(
    category=category,
    user_context=user,
    cache_service=cache_service,
    table_config={
        "page": page,
        "page_size": page_size,
        "sort_by": sort_by,
        "sort_direction": sort_direction
    }
)
```

#### 3. Error Handling Strategy
```python
try:
    # Route logic here
    return response
except HTTPException:
    raise  # Re-raise FastAPI exceptions
except Exception as e:
    error(f"Financial API error: {str(e)}")  # Log via notification system
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Internal server error: {str(e)}"
    )
```

### Dependency Injection

#### 1. User Authentication
```python
def get_current_user() -> dict:
    """FastAPI dependency for authenticated user context."""
    user_context = get_current_user_context()
    if not user_context:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user_context
```

#### 2. Cache Service
```python
def get_cache_service():
    """FastAPI dependency for cache service injection."""
    # Production would inject actual Redis cache
    # Test environment uses mock cache
    return cache_service_instance
```

## API Endpoints

### Market Data API
- **Endpoint**: `GET /api/v1/financial/market-data/{category}`
- **Purpose**: Retrieve paginated market data for a specific category
- **Parameters**: category, page, page_size, sort_by, sort_direction, include_cache
- **Authentication**: Required
- **Caching**: 5-minute TTL with Redis
- **Response**: TableV2-formatted data with pagination metadata

### Data Transformation API
- **Endpoint**: `POST /api/v1/financial/transform`
- **Purpose**: Convert raw ticker data to TableV2 format
- **Body**: List of ticker data objects with table configuration
- **Authentication**: Required
- **Validation**: Pydantic model validation for ticker data structure
- **Response**: Transformed table data with conversion statistics

### Performance Metrics API
- **Endpoint**: `GET /api/v1/financial/metrics/{symbol}`
- **Purpose**: Calculate performance metrics for a ticker symbol
- **Parameters**: symbol, include_historical, metrics_type
- **Authentication**: Required
- **Response**: Performance metrics including percentage changes and volume

### Financial Formatting API
- **Endpoint**: `POST /api/v1/financial/format`
- **Purpose**: Format financial values for display
- **Body**: Raw financial data with formatting options
- **Authentication**: Required
- **Features**: Locale-aware formatting, currency conversion
- **Response**: Formatted financial values with metadata

### Bulk Processing API
- **Endpoint**: `POST /api/v1/financial/bulk`
- **Purpose**: Process multiple market categories in parallel
- **Body**: List of categories with processing configuration
- **Authentication**: Required
- **Features**: Parallel processing with configurable worker count
- **Response**: Aggregated results with processing summary

### Health Check API
- **Endpoint**: `GET /api/v1/financial/health`
- **Purpose**: Monitor service health and performance
- **Authentication**: Not required
- **Features**: Service status, performance metrics, uptime tracking
- **Response**: Comprehensive health status

## Request/Response Flow

### 1. Request Processing
```
HTTP Request → FastAPI Router → Route Handler → Dependencies → Controller → Service → Domain
```

### 2. Response Building
```
Domain → Service → Controller → Route Handler → Pydantic Model → HTTP Response
```

### 3. Error Handling
```
Exception → Route Handler → Notification System → HTTPException → Error Response
```

## Validation and Security

### Input Validation
- **Pydantic Models**: Automatic request/response validation
- **Field Validators**: Custom validation for business rules
- **Type Safety**: Full type checking with mypy compatibility

### Authentication
- **FastAPI Dependencies**: User context injection
- **Token Validation**: Integration with existing auth middleware
- **Authorization**: Permission-based access control

### Rate Limiting
- **Middleware Integration**: Existing Redis-based rate limiting
- **Endpoint-Specific Limits**: Configurable per endpoint
- **Error Responses**: Proper 429 responses with retry headers

## Performance Features

### Caching Strategy
- **Redis Backend**: 5-minute TTL for market data
- **Cache Headers**: Proper HTTP cache control headers
- **Cache Invalidation**: Event-driven cache clearing

### Pagination
- **Query Parameters**: page, page_size with sensible defaults
- **Metadata**: Total count, page count, navigation links
- **Performance**: Database-level LIMIT/OFFSET optimization

### Async Processing
- **Async Route Handlers**: Non-blocking I/O operations
- **Bulk Operations**: Parallel processing with asyncio
- **Database Connections**: Async SQLAlchemy sessions

## Integration Points

### Controller Layer
- **Function Interface**: Pure function calls with dependency injection
- **Error Propagation**: Consistent error handling across layers
- **Data Transformation**: Controller handles business logic

### Notification System
- **Context Setting**: Request context for user notifications
- **Logging Integration**: Structured logging with error tracking
- **User Feedback**: Success/error messages for UI

### Cache Service
- **Redis Integration**: Distributed caching for performance
- **Dependency Injection**: Testable cache service abstraction
- **TTL Management**: Configurable cache expiration

## Testing Strategy

### BDD Tests
- **pytest-BDD**: Behavior-driven development with Gherkin scenarios
- **Mock Integration**: Comprehensive mocking of dependencies
- **Fixture Management**: Reusable test fixtures for all scenarios

### Test Coverage
- **Route Handlers**: Direct endpoint testing
- **Error Scenarios**: Authentication, validation, service failures
- **Integration Tests**: Full request/response cycle testing

## Deployment Considerations

### Health Monitoring
- **Health Endpoint**: Service status monitoring
- **Performance Metrics**: Response time, memory usage, CPU usage
- **Service Dependencies**: Database, cache, external API health

### Scaling
- **Stateless Design**: No server-side session state
- **Horizontal Scaling**: Load balancer compatible
- **Resource Efficiency**: Async processing for high concurrency

### Configuration
- **Environment Variables**: Service configuration
- **Feature Flags**: Runtime behavior control
- **Cache Configuration**: TTL and connection settings

## Future Enhancements

### API Versioning
- **URL Versioning**: /api/v2/financial/ for breaking changes
- **Backward Compatibility**: Support for multiple API versions
- **Deprecation Strategy**: Gradual sunset of old versions

### Advanced Features
- **WebSocket Support**: Real-time market data streaming
- **GraphQL Integration**: Flexible query interface
- **OpenAPI Documentation**: Auto-generated API docs

### Performance Optimization
- **Database Indexing**: Optimized queries for common access patterns
- **CDN Integration**: Static asset caching
- **Compression**: Response compression for large datasets

## Conclusion

The financial routes architecture provides a robust, scalable foundation for TableV2's financial data API. The function-based design ensures maintainability while the clean architecture patterns enable easy testing and future enhancements. The comprehensive validation, error handling, and performance features make this a production-ready implementation.