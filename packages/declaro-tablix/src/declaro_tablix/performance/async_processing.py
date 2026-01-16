"""
Async processing module for TableV2 with Python 3.13 no-GIL optimizations.

This module provides high-performance async processing capabilities for large datasets,
leveraging Python 3.13's free-threaded execution model and true parallelism.
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import TableData
from declaro_tablix.repositories.table_data_repository import get_table_data

# Performance tracking
_performance_metrics = {
    "async_operations": 0,
    "parallel_operations": 0,
    "streaming_operations": 0,
    "total_processing_time": 0.0,
    "memory_optimized_operations": 0,
    "concurrent_operations": 0,
    "last_reset": datetime.utcnow(),
}

# Configuration for async processing
ASYNC_CONFIG = {
    "max_workers": 8,  # Optimal for no-GIL performance
    "chunk_size": 1000,  # Size of data chunks for processing
    "stream_buffer_size": 100,  # Buffer size for streaming
    "parallel_threshold": 5000,  # Minimum rows for parallel processing
    "memory_threshold": 10000,  # Rows threshold for memory optimization
    "concurrent_table_limit": 5,  # Max concurrent table operations
}


def performance_tracker(operation_type: str):
    """Decorator to track performance metrics for async operations."""

    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            _performance_metrics[f"{operation_type}_operations"] += 1

            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                _performance_metrics["total_processing_time"] += time.time() - start_time

        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            _performance_metrics[f"{operation_type}_operations"] += 1

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                _performance_metrics["total_processing_time"] += time.time() - start_time

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


@performance_tracker("async")
async def process_large_dataset_async(
    table_name: str, user_id: str, processing_function: callable, batch_size: int = None, max_workers: int = None
) -> Dict[str, Any]:
    """
    Process large datasets asynchronously with true parallelism.

    Args:
        table_name: Name of the table to process
        user_id: User ID for data access
        processing_function: Function to apply to each data chunk
        batch_size: Size of data batches (default: from config)
        max_workers: Number of worker threads (default: from config)

    Returns:
        Dictionary with processing results and performance metrics
    """
    try:
        info(f"Starting async processing for table '{table_name}' with user '{user_id}'")

        batch_size = batch_size or ASYNC_CONFIG["chunk_size"]
        max_workers = max_workers or ASYNC_CONFIG["max_workers"]

        start_time = time.time()

        # Get table data
        table_data = await get_table_data_async(table_name, user_id)
        if not table_data:
            error(f"No data found for table '{table_name}'")
            return {"error": "No data found", "processed_rows": 0}

        total_rows = len(table_data.rows)

        # Check if parallel processing is beneficial
        if total_rows < ASYNC_CONFIG["parallel_threshold"]:
            info(f"Dataset size ({total_rows}) below parallel threshold, using single-threaded processing")
            result = await processing_function(table_data.rows)
            return {
                "processed_rows": total_rows,
                "processing_time": time.time() - start_time,
                "method": "single_threaded",
                "result": result,
            }

        # Create data chunks for parallel processing
        chunks = create_data_chunks(table_data.rows, batch_size)
        info(f"Created {len(chunks)} chunks for parallel processing")

        # Process chunks in parallel using ThreadPoolExecutor
        # Python 3.13 no-GIL allows true parallelism for CPU-bound tasks
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks for processing
            futures = []
            for i, chunk in enumerate(chunks):
                future = executor.submit(process_chunk_sync, chunk, processing_function, i)
                futures.append(future)

            # Collect results as they complete
            results = []
            for future in asyncio.as_completed([asyncio.wrap_future(f) for f in futures]):
                chunk_result = await future
                results.append(chunk_result)

        # Combine results
        combined_result = combine_chunk_results(results)
        processing_time = time.time() - start_time

        success(f"Async processing completed: {total_rows} rows in {processing_time:.2f}s")

        return {
            "processed_rows": total_rows,
            "processing_time": processing_time,
            "method": "parallel",
            "chunks_processed": len(chunks),
            "workers_used": max_workers,
            "result": combined_result,
            "performance_gain": calculate_performance_gain(total_rows, processing_time),
        }

    except Exception as e:
        error(f"Error in async processing: {str(e)}")
        return {"error": str(e), "processed_rows": 0}


@performance_tracker("streaming")
async def stream_large_table_data(
    table_name: str, user_id: str, filters: Optional[Dict[str, Any]] = None, page_size: int = None
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream large table data asynchronously to avoid memory issues.

    Args:
        table_name: Name of the table to stream
        user_id: User ID for data access
        filters: Optional filters to apply
        page_size: Size of each streamed batch

    Yields:
        Batches of table data
    """
    try:
        info(f"Starting data streaming for table '{table_name}'")

        page_size = page_size or ASYNC_CONFIG["stream_buffer_size"]
        offset = 0

        while True:
            # Get next batch of data
            batch_data = await get_table_data_batch_async(table_name, user_id, offset, page_size, filters)

            if not batch_data or len(batch_data.rows) == 0:
                break

            # Yield batch with metadata
            yield {
                "data": batch_data.rows,
                "offset": offset,
                "batch_size": len(batch_data.rows),
                "table_name": table_name,
                "timestamp": datetime.utcnow().isoformat(),
            }

            offset += len(batch_data.rows)

            # Small delay to prevent overwhelming the client
            await asyncio.sleep(0.01)

        success(f"Streaming completed for table '{table_name}': {offset} total rows")

    except Exception as e:
        error(f"Error in streaming: {str(e)}")
        yield {"error": str(e), "offset": offset}


@performance_tracker("concurrent")
async def process_multiple_tables_concurrent(
    table_operations: List[Dict[str, Any]], max_concurrent: int = None
) -> Dict[str, Any]:
    """
    Process multiple table operations concurrently.

    Args:
        table_operations: List of table operations to process
        max_concurrent: Maximum concurrent operations (default: from config)

    Returns:
        Dictionary with results from all operations
    """
    try:
        max_concurrent = max_concurrent or ASYNC_CONFIG["concurrent_table_limit"]

        info(f"Starting concurrent processing of {len(table_operations)} table operations")

        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_single_operation(operation: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                table_name = operation.get("table_name")
                user_id = operation.get("user_id")
                operation_type = operation.get("operation_type")

                start_time = time.time()

                try:
                    if operation_type == "process_data":
                        result = await process_large_dataset_async(table_name, user_id, operation.get("processing_function"))
                    elif operation_type == "stream_data":
                        result = {"stream_initiated": True}
                        # For streaming, we just indicate that streaming can start
                    elif operation_type == "get_data":
                        result = await get_table_data_async(table_name, user_id)
                    else:
                        return {
                            "table_name": table_name,
                            "operation_type": operation_type,
                            "error": f"Unknown operation type: {operation_type}",
                            "processing_time": time.time() - start_time,
                            "status": "error",
                        }

                    return {
                        "table_name": table_name,
                        "operation_type": operation_type,
                        "result": result,
                        "processing_time": time.time() - start_time,
                        "status": "success",
                    }

                except Exception as e:
                    error(f"Error processing {operation_type} for {table_name}: {str(e)}")
                    return {
                        "table_name": table_name,
                        "operation_type": operation_type,
                        "error": str(e),
                        "processing_time": time.time() - start_time,
                        "status": "error",
                    }

        # Process all operations concurrently
        start_time = time.time()
        results = await asyncio.gather(*[process_single_operation(op) for op in table_operations], return_exceptions=True)

        # Separate successful and failed operations
        successful_operations = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
        failed_operations = [r for r in results if isinstance(r, dict) and r.get("status") == "error"]

        total_time = time.time() - start_time

        success(f"Concurrent processing completed: {len(successful_operations)} successful, {len(failed_operations)} failed")

        return {
            "total_operations": len(table_operations),
            "successful_operations": len(successful_operations),
            "failed_operations": len(failed_operations),
            "total_processing_time": total_time,
            "average_time_per_operation": total_time / len(table_operations) if table_operations else 0,
            "results": results,
        }

    except Exception as e:
        error(f"Error in concurrent processing: {str(e)}")
        return {"error": str(e), "total_operations": len(table_operations)}


@performance_tracker("memory")
async def process_with_memory_optimization(
    table_name: str, user_id: str, processing_function: callable, memory_limit_mb: int = 100
) -> Dict[str, Any]:
    """
    Process data with memory optimization using generators.

    Args:
        table_name: Name of the table to process
        user_id: User ID for data access
        processing_function: Function to apply to data
        memory_limit_mb: Memory limit in MB

    Returns:
        Processing results with memory usage stats
    """
    try:
        info(f"Starting memory-optimized processing for table '{table_name}'")

        start_time = time.time()
        initial_memory = get_memory_usage()

        # Process data in streaming fashion to minimize memory usage
        total_processed = 0
        results = []

        async for batch in stream_large_table_data(table_name, user_id):
            if "error" in batch:
                error(f"Error in streaming: {batch['error']}")
                break

            # Process batch
            batch_result = await processing_function(batch["data"])
            results.append(batch_result)
            total_processed += batch["batch_size"]

            # Check memory usage
            current_memory = get_memory_usage()
            memory_used = current_memory - initial_memory

            if memory_used > memory_limit_mb:
                warning(f"Memory limit exceeded: {memory_used}MB > {memory_limit_mb}MB")
                # Implement memory cleanup if needed
                await cleanup_memory()

        final_memory = get_memory_usage()
        processing_time = time.time() - start_time

        success(f"Memory-optimized processing completed: {total_processed} rows processed")

        return {
            "processed_rows": total_processed,
            "processing_time": processing_time,
            "memory_usage": {
                "initial_mb": initial_memory,
                "final_mb": final_memory,
                "peak_usage_mb": final_memory - initial_memory,
                "limit_mb": memory_limit_mb,
            },
            "results": results,
        }

    except Exception as e:
        error(f"Error in memory-optimized processing: {str(e)}")
        return {"error": str(e), "processed_rows": 0}


# Helper functions for async processing


async def get_table_data_async(table_name: str, user_id: str) -> Optional[TableData]:
    """Async wrapper for getting table data."""
    try:
        # Run the sync function in a thread pool
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, get_table_data, table_name, user_id)
    except Exception as e:
        error(f"Error getting table data async: {str(e)}")
        return None


async def get_table_data_batch_async(
    table_name: str, user_id: str, offset: int, limit: int, filters: Optional[Dict[str, Any]] = None
) -> Optional[TableData]:
    """Get a batch of table data asynchronously."""
    try:
        # This would implement actual batched data retrieval
        # For now, we'll simulate with a subset of data
        full_data = await get_table_data_async(table_name, user_id)
        if not full_data:
            return None

        # Simulate pagination
        start_idx = offset
        end_idx = min(offset + limit, len(full_data.rows))

        if start_idx >= len(full_data.rows):
            return TableData(table_name=table_name, columns=full_data.columns, rows=[], metadata=full_data.metadata)

        batch_rows = full_data.rows[start_idx:end_idx]

        return TableData(table_name=table_name, columns=full_data.columns, rows=batch_rows, metadata=full_data.metadata)

    except Exception as e:
        error(f"Error getting batch data: {str(e)}")
        return None


def create_data_chunks(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """Create chunks of data for parallel processing."""
    chunks = []
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        chunks.append(chunk)
    return chunks


def process_chunk_sync(chunk: List[Any], processing_function: callable, chunk_index: int) -> Dict[str, Any]:
    """Process a single chunk synchronously (called from thread pool)."""
    try:
        start_time = time.time()
        result = processing_function(chunk)
        processing_time = time.time() - start_time

        return {
            "chunk_index": chunk_index,
            "chunk_size": len(chunk),
            "processing_time": processing_time,
            "result": result,
            "status": "success",
        }
    except Exception as e:
        return {"chunk_index": chunk_index, "chunk_size": len(chunk), "error": str(e), "status": "error"}


def combine_chunk_results(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Combine results from multiple chunks."""
    successful_chunks = [r for r in chunk_results if r.get("status") == "success"]
    failed_chunks = [r for r in chunk_results if r.get("status") == "error"]

    total_processing_time = sum(r.get("processing_time", 0) for r in chunk_results)
    total_rows = sum(r.get("chunk_size", 0) for r in chunk_results)

    return {
        "total_chunks": len(chunk_results),
        "successful_chunks": len(successful_chunks),
        "failed_chunks": len(failed_chunks),
        "total_rows": total_rows,
        "total_processing_time": total_processing_time,
        "results": [r.get("result") for r in successful_chunks if r.get("result")],
    }


def calculate_performance_gain(total_rows: int, processing_time: float) -> Dict[str, Any]:
    """Calculate performance gain metrics."""
    rows_per_second = total_rows / processing_time if processing_time > 0 else 0

    # Estimate sequential processing time (rough approximation)
    estimated_sequential_time = processing_time * 1.5  # Assume 50% overhead for sequential

    return {
        "rows_per_second": round(rows_per_second, 2),
        "estimated_sequential_time": round(estimated_sequential_time, 2),
        "estimated_speedup": round(estimated_sequential_time / processing_time, 2) if processing_time > 0 else 1,
        "efficiency_rating": "high" if rows_per_second > 1000 else "medium" if rows_per_second > 100 else "low",
    }


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil

        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # Convert to MB
    except ImportError:
        # Fallback if psutil not available
        return 0.0


async def cleanup_memory():
    """Cleanup memory by forcing garbage collection."""
    import gc

    gc.collect()
    await asyncio.sleep(0.1)  # Small delay for cleanup


def get_async_performance_metrics() -> Dict[str, Any]:
    """Get performance metrics for async operations."""
    total_operations = (
        _performance_metrics["async_operations"]
        + _performance_metrics["parallel_operations"]
        + _performance_metrics["streaming_operations"]
        + _performance_metrics["concurrent_operations"]
    )

    avg_processing_time = _performance_metrics["total_processing_time"] / total_operations if total_operations > 0 else 0

    uptime = datetime.utcnow() - _performance_metrics["last_reset"]

    return {
        "operations": {
            "async_operations": _performance_metrics["async_operations"],
            "parallel_operations": _performance_metrics["parallel_operations"],
            "streaming_operations": _performance_metrics["streaming_operations"],
            "concurrent_operations": _performance_metrics["concurrent_operations"],
            "memory_optimized_operations": _performance_metrics["memory_optimized_operations"],
            "total_operations": total_operations,
        },
        "performance": {
            "total_processing_time": round(_performance_metrics["total_processing_time"], 2),
            "average_processing_time": round(avg_processing_time, 2),
            "operations_per_second": (
                round(total_operations / uptime.total_seconds(), 2) if uptime.total_seconds() > 0 else 0
            ),
        },
        "uptime": {"seconds": uptime.total_seconds(), "human_readable": str(uptime)},
        "configuration": ASYNC_CONFIG,
    }


def reset_async_performance_metrics():
    """Reset performance metrics."""
    _performance_metrics.update(
        {
            "async_operations": 0,
            "parallel_operations": 0,
            "streaming_operations": 0,
            "total_processing_time": 0.0,
            "memory_optimized_operations": 0,
            "concurrent_operations": 0,
            "last_reset": datetime.utcnow(),
        }
    )
    info("Async performance metrics reset")


# FastAPI dependency injection functions
def get_async_processor_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for async processing functions."""
    return {
        "process_large_dataset_async": process_large_dataset_async,
        "stream_large_table_data": stream_large_table_data,
        "process_multiple_tables_concurrent": process_multiple_tables_concurrent,
        "process_with_memory_optimization": process_with_memory_optimization,
        "get_async_performance_metrics": get_async_performance_metrics,
        "reset_async_performance_metrics": reset_async_performance_metrics,
    }


def get_streaming_processor_for_dependency() -> callable:
    """FastAPI dependency for streaming operations."""
    return stream_large_table_data


def get_concurrent_processor_for_dependency() -> callable:
    """FastAPI dependency for concurrent operations."""
    return process_multiple_tables_concurrent


def get_memory_optimizer_for_dependency() -> callable:
    """FastAPI dependency for memory-optimized operations."""
    return process_with_memory_optimization
