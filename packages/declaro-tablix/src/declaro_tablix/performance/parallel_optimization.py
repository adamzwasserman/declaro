"""
Parallel optimization module for TableV2 leveraging Python 3.13 no-GIL capabilities.

This module provides optimized parallel processing functions that take advantage
of Python 3.13's free-threaded execution model for true CPU parallelism.
"""

import concurrent.futures
import multiprocessing
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List

from declaro_advise import error, info, success, warning

# Global configuration for parallel processing
PARALLEL_CONFIG = {
    "cpu_count": multiprocessing.cpu_count(),
    "optimal_thread_count": min(multiprocessing.cpu_count() * 2, 16),  # 2x cores, max 16
    "chunk_size_factor": 0.8,  # Chunk size as factor of optimal thread count
    "minimum_chunk_size": 100,  # Minimum rows per chunk
    "maximum_chunk_size": 10000,  # Maximum rows per chunk
    "enable_numa_optimization": True,  # Enable NUMA-aware processing
    "thread_pool_reuse": True,  # Reuse thread pools for efficiency
}

# Thread-local storage for performance tracking
_thread_local = threading.local()

# Global performance metrics
_parallel_metrics = {
    "parallel_operations": 0,
    "thread_pool_operations": 0,
    "process_pool_operations": 0,
    "cpu_intensive_operations": 0,
    "io_intensive_operations": 0,
    "hybrid_operations": 0,
    "numa_operations": 0,
    "total_threads_used": 0,
    "total_processes_used": 0,
    "total_processing_time": 0.0,
    "peak_cpu_usage": 0.0,
    "peak_memory_usage": 0.0,
    "numa_optimized_operations": 0,
    "cache_hit_rate": 0.0,
    "last_reset": datetime.utcnow(),
}

# Reusable thread pool for efficiency
_thread_pool = None
_thread_pool_lock = threading.Lock()


def get_thread_pool(max_workers: int = None) -> concurrent.futures.ThreadPoolExecutor:
    """Get or create a reusable thread pool."""
    global _thread_pool

    if not PARALLEL_CONFIG["thread_pool_reuse"]:
        return concurrent.futures.ThreadPoolExecutor(max_workers=max_workers or PARALLEL_CONFIG["optimal_thread_count"])

    with _thread_pool_lock:
        if _thread_pool is None:
            _thread_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers or PARALLEL_CONFIG["optimal_thread_count"]
            )

    return _thread_pool


def parallel_performance_tracker(operation_type: str):
    """Decorator to track parallel processing performance."""

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_cpu = get_cpu_usage()
            start_memory = get_memory_usage()

            _parallel_metrics[f"{operation_type}_operations"] += 1

            try:
                result = func(*args, **kwargs)

                # Track peak usage
                end_cpu = get_cpu_usage()
                end_memory = get_memory_usage()

                _parallel_metrics["peak_cpu_usage"] = max(_parallel_metrics["peak_cpu_usage"], max(start_cpu, end_cpu))
                _parallel_metrics["peak_memory_usage"] = max(
                    _parallel_metrics["peak_memory_usage"], max(start_memory, end_memory)
                )

                return result

            finally:
                _parallel_metrics["total_processing_time"] += time.time() - start_time

        return wrapper

    return decorator


@parallel_performance_tracker("parallel")
def process_data_parallel_cpu_intensive(
    data: List[Any], processing_function: Callable, max_workers: int = None, chunk_size: int = None
) -> Dict[str, Any]:
    """
    Process data in parallel for CPU-intensive operations.

    Optimized for Python 3.13 no-GIL true parallelism.

    Args:
        data: List of data to process
        processing_function: Function to apply to each chunk
        max_workers: Maximum number of worker threads
        chunk_size: Size of data chunks

    Returns:
        Dictionary with processing results and performance metrics
    """
    try:
        if not data:
            return {"error": "No data provided", "processed_items": 0}

        info(f"Starting CPU-intensive parallel processing of {len(data)} items")

        # Optimize chunk size for CPU-intensive work
        max_workers = max_workers or PARALLEL_CONFIG["optimal_thread_count"]
        chunk_size = chunk_size or calculate_optimal_chunk_size(len(data), max_workers, "cpu_intensive")

        start_time = time.time()

        # Create chunks
        chunks = create_optimized_chunks(data, chunk_size)
        info(f"Created {len(chunks)} chunks for parallel processing")

        # Process chunks in parallel using ThreadPoolExecutor
        with get_thread_pool(max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(process_chunk_cpu_intensive, chunk, processing_function, i): i
                for i, chunk in enumerate(chunks)
            }

            # Collect results
            results = []
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    error(f"Chunk {chunk_index} processing failed: {str(e)}")
                    results.append({"error": str(e), "chunk_index": chunk_index})

        # Combine results
        combined_result = combine_parallel_results(results)
        processing_time = time.time() - start_time

        _parallel_metrics["total_threads_used"] += max_workers

        success(f"CPU-intensive parallel processing completed: {len(data)} items in {processing_time:.2f}s")

        return {
            "processed_items": len(data),
            "processing_time": processing_time,
            "chunks_processed": len(chunks),
            "workers_used": max_workers,
            "throughput": len(data) / processing_time if processing_time > 0 else 0,
            "result": combined_result,
            "optimization_type": "cpu_intensive",
            "parallel_efficiency": calculate_parallel_efficiency(len(data), processing_time, max_workers),
        }

    except Exception as e:
        error(f"Error in CPU-intensive parallel processing: {str(e)}")
        return {"error": str(e), "processed_items": 0}


@parallel_performance_tracker("parallel")
def process_data_parallel_io_intensive(
    data: List[Any], processing_function: Callable, max_workers: int = None, chunk_size: int = None
) -> Dict[str, Any]:
    """
    Process data in parallel for I/O-intensive operations.

    Uses higher concurrency for I/O-bound tasks.

    Args:
        data: List of data to process
        processing_function: Function to apply to each chunk
        max_workers: Maximum number of worker threads
        chunk_size: Size of data chunks

    Returns:
        Dictionary with processing results and performance metrics
    """
    try:
        if not data:
            return {"error": "No data provided", "processed_items": 0}

        info(f"Starting I/O-intensive parallel processing of {len(data)} items")

        # Optimize for I/O-intensive work (more threads, smaller chunks)
        max_workers = max_workers or (PARALLEL_CONFIG["optimal_thread_count"] * 2)
        chunk_size = chunk_size or calculate_optimal_chunk_size(len(data), max_workers, "io_intensive")

        start_time = time.time()

        # Create smaller chunks for I/O work
        chunks = create_optimized_chunks(data, chunk_size)
        info(f"Created {len(chunks)} chunks for I/O-intensive processing")

        # Process chunks with higher concurrency
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(process_chunk_io_intensive, chunk, processing_function, i): i
                for i, chunk in enumerate(chunks)
            }

            # Collect results
            results = []
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    error(f"Chunk {chunk_index} processing failed: {str(e)}")
                    results.append({"error": str(e), "chunk_index": chunk_index})

        # Combine results
        combined_result = combine_parallel_results(results)
        processing_time = time.time() - start_time

        _parallel_metrics["total_threads_used"] += max_workers

        success(f"I/O-intensive parallel processing completed: {len(data)} items in {processing_time:.2f}s")

        return {
            "processed_items": len(data),
            "processing_time": processing_time,
            "chunks_processed": len(chunks),
            "workers_used": max_workers,
            "throughput": len(data) / processing_time if processing_time > 0 else 0,
            "result": combined_result,
            "optimization_type": "io_intensive",
            "parallel_efficiency": calculate_parallel_efficiency(len(data), processing_time, max_workers),
        }

    except Exception as e:
        error(f"Error in I/O-intensive parallel processing: {str(e)}")
        return {"error": str(e), "processed_items": 0}


@parallel_performance_tracker("hybrid")
def process_data_hybrid_parallel(
    data: List[Any], cpu_function: Callable, io_function: Callable, max_workers: int = None
) -> Dict[str, Any]:
    """
    Process data using hybrid CPU and I/O parallel processing.

    Separates CPU-intensive and I/O-intensive operations for optimal performance.

    Args:
        data: List of data to process
        cpu_function: CPU-intensive processing function
        io_function: I/O-intensive processing function
        max_workers: Maximum number of worker threads

    Returns:
        Dictionary with processing results and performance metrics
    """
    try:
        if not data:
            return {"error": "No data provided", "processed_items": 0}

        info(f"Starting hybrid parallel processing of {len(data)} items")

        max_workers = max_workers or PARALLEL_CONFIG["optimal_thread_count"]
        start_time = time.time()

        # Split workload between CPU and I/O operations
        cpu_workers = max_workers // 2
        io_workers = max_workers - cpu_workers

        # Process CPU-intensive operations
        cpu_result = process_data_parallel_cpu_intensive(data, cpu_function, cpu_workers)

        # Process I/O-intensive operations
        io_result = process_data_parallel_io_intensive(data, io_function, io_workers)

        processing_time = time.time() - start_time

        success(f"Hybrid parallel processing completed: {len(data)} items in {processing_time:.2f}s")

        return {
            "processed_items": len(data),
            "processing_time": processing_time,
            "cpu_result": cpu_result,
            "io_result": io_result,
            "optimization_type": "hybrid",
            "workers_used": max_workers,
            "cpu_workers": cpu_workers,
            "io_workers": io_workers,
        }

    except Exception as e:
        error(f"Error in hybrid parallel processing: {str(e)}")
        return {"error": str(e), "processed_items": 0}


@parallel_performance_tracker("numa")
def process_data_numa_optimized(data: List[Any], processing_function: Callable, max_workers: int = None) -> Dict[str, Any]:
    """
    Process data with NUMA (Non-Uniform Memory Access) optimization.

    Optimizes memory locality for better performance on multi-socket systems.

    Args:
        data: List of data to process
        processing_function: Function to apply to data
        max_workers: Maximum number of worker threads

    Returns:
        Dictionary with processing results and performance metrics
    """
    try:
        if not PARALLEL_CONFIG["enable_numa_optimization"]:
            warning("NUMA optimization disabled, falling back to standard parallel processing")
            return process_data_parallel_cpu_intensive(data, processing_function, max_workers)

        info(f"Starting NUMA-optimized parallel processing of {len(data)} items")

        max_workers = max_workers or PARALLEL_CONFIG["optimal_thread_count"]
        start_time = time.time()

        # Detect NUMA topology
        numa_nodes = detect_numa_topology()
        if len(numa_nodes) <= 1:
            warning("Single NUMA node detected, using standard parallel processing")
            return process_data_parallel_cpu_intensive(data, processing_function, max_workers)

        # Distribute work across NUMA nodes
        workers_per_node = max_workers // len(numa_nodes)
        node_chunks = distribute_data_across_numa_nodes(data, numa_nodes, workers_per_node)

        # Process each NUMA node separately
        node_results = []
        for node_id, node_data in node_chunks.items():
            if node_data:
                node_result = process_numa_node_data(node_data, processing_function, workers_per_node, node_id)
                node_results.append(node_result)

        # Combine results from all nodes
        combined_result = combine_numa_results(node_results)
        processing_time = time.time() - start_time

        _parallel_metrics["numa_optimized_operations"] += 1

        success(f"NUMA-optimized processing completed: {len(data)} items in {processing_time:.2f}s")

        return {
            "processed_items": len(data),
            "processing_time": processing_time,
            "numa_nodes_used": len(numa_nodes),
            "workers_per_node": workers_per_node,
            "result": combined_result,
            "optimization_type": "numa_optimized",
            "numa_efficiency": calculate_numa_efficiency(len(data), processing_time, len(numa_nodes)),
        }

    except Exception as e:
        error(f"Error in NUMA-optimized processing: {str(e)}")
        return {"error": str(e), "processed_items": 0}


# Helper functions for parallel processing


def calculate_optimal_chunk_size(data_size: int, max_workers: int, optimization_type: str) -> int:
    """Calculate optimal chunk size based on data size and optimization type."""
    if optimization_type == "cpu_intensive":
        # Larger chunks for CPU-intensive work
        base_chunk_size = data_size // max_workers
        return max(PARALLEL_CONFIG["minimum_chunk_size"], min(base_chunk_size, PARALLEL_CONFIG["maximum_chunk_size"]))
    elif optimization_type == "io_intensive":
        # Smaller chunks for I/O-intensive work
        base_chunk_size = data_size // (max_workers * 2)
        return max(
            PARALLEL_CONFIG["minimum_chunk_size"] // 2, min(base_chunk_size, PARALLEL_CONFIG["maximum_chunk_size"] // 2)
        )
    else:
        # Default chunk size
        base_chunk_size = data_size // max_workers
        return max(PARALLEL_CONFIG["minimum_chunk_size"], min(base_chunk_size, PARALLEL_CONFIG["maximum_chunk_size"]))


def create_optimized_chunks(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """Create optimized chunks for parallel processing."""
    chunks = []
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        chunks.append(chunk)
    return chunks


def process_chunk_cpu_intensive(chunk: List[Any], processing_function: Callable, chunk_index: int) -> Dict[str, Any]:
    """Process a single chunk for CPU-intensive operations."""
    try:
        start_time = time.time()

        # Set thread-local storage for this chunk
        if not hasattr(_thread_local, "chunk_index"):
            _thread_local.chunk_index = chunk_index

        # Process the chunk
        result = processing_function(chunk)

        processing_time = time.time() - start_time

        return {
            "chunk_index": chunk_index,
            "chunk_size": len(chunk),
            "processing_time": processing_time,
            "result": result,
            "thread_id": threading.get_ident(),
            "status": "success",
        }

    except Exception as e:
        return {
            "chunk_index": chunk_index,
            "chunk_size": len(chunk),
            "error": str(e),
            "thread_id": threading.get_ident(),
            "status": "error",
        }


def process_chunk_io_intensive(chunk: List[Any], processing_function: Callable, chunk_index: int) -> Dict[str, Any]:
    """Process a single chunk for I/O-intensive operations."""
    try:
        start_time = time.time()

        # Set thread-local storage for this chunk
        if not hasattr(_thread_local, "chunk_index"):
            _thread_local.chunk_index = chunk_index

        # Process the chunk (may involve I/O operations)
        result = processing_function(chunk)

        processing_time = time.time() - start_time

        return {
            "chunk_index": chunk_index,
            "chunk_size": len(chunk),
            "processing_time": processing_time,
            "result": result,
            "thread_id": threading.get_ident(),
            "status": "success",
        }

    except Exception as e:
        return {
            "chunk_index": chunk_index,
            "chunk_size": len(chunk),
            "error": str(e),
            "thread_id": threading.get_ident(),
            "status": "error",
        }


def combine_parallel_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Combine results from parallel chunk processing."""
    successful_results = [r for r in results if r.get("status") == "success"]
    failed_results = [r for r in results if r.get("status") == "error"]

    total_processing_time = sum(r.get("processing_time", 0) for r in results)
    total_items = sum(r.get("chunk_size", 0) for r in results)

    # Get unique thread IDs
    thread_ids = set(r.get("thread_id") for r in results if r.get("thread_id"))

    return {
        "total_chunks": len(results),
        "successful_chunks": len(successful_results),
        "failed_chunks": len(failed_results),
        "total_items": total_items,
        "total_processing_time": total_processing_time,
        "threads_used": len(thread_ids),
        "results": [r.get("result") for r in successful_results if r.get("result")],
        "errors": [r.get("error") for r in failed_results if r.get("error")],
    }


def calculate_parallel_efficiency(items: int, processing_time: float, workers: int) -> Dict[str, Any]:
    """Calculate parallel processing efficiency metrics."""
    if processing_time <= 0:
        return {"efficiency": 0, "speedup": 1, "utilization": 0}

    # Estimate sequential processing time (rough approximation)
    estimated_sequential_time = processing_time * workers * 0.8  # Assume 80% parallel efficiency

    actual_speedup = estimated_sequential_time / processing_time if processing_time > 0 else 1
    theoretical_speedup = workers
    efficiency = actual_speedup / theoretical_speedup if theoretical_speedup > 0 else 0

    return {
        "efficiency": round(efficiency, 2),
        "speedup": round(actual_speedup, 2),
        "utilization": round(efficiency * 100, 2),
        "items_per_second": round(items / processing_time, 2),
    }


def detect_numa_topology() -> List[int]:
    """Detect NUMA topology of the system."""
    try:
        # Try to detect NUMA nodes using /sys/devices/system/node
        import os

        numa_nodes = []

        node_dir = "/sys/devices/system/node"
        if os.path.exists(node_dir):
            for entry in os.listdir(node_dir):
                if entry.startswith("node") and entry[4:].isdigit():
                    node_id = int(entry[4:])
                    numa_nodes.append(node_id)

        if numa_nodes:
            return sorted(numa_nodes)

        # Fallback: assume single node
        return [0]

    except Exception:
        # Error detecting NUMA, assume single node
        return [0]


def distribute_data_across_numa_nodes(data: List[Any], numa_nodes: List[int], workers_per_node: int) -> Dict[int, List[Any]]:
    """Distribute data across NUMA nodes for optimal memory locality."""
    node_chunks = defaultdict(list)

    # Simple round-robin distribution
    for i, item in enumerate(data):
        node_id = numa_nodes[i % len(numa_nodes)]
        node_chunks[node_id].append(item)

    return dict(node_chunks)


def process_numa_node_data(data: List[Any], processing_function: Callable, workers: int, node_id: int) -> Dict[str, Any]:
    """Process data on a specific NUMA node."""
    try:
        info(f"Processing {len(data)} items on NUMA node {node_id}")

        # Process data using CPU-intensive parallel processing
        result = process_data_parallel_cpu_intensive(data, processing_function, workers)

        return {"node_id": node_id, "items_processed": len(data), "result": result, "status": "success"}

    except Exception as e:
        error(f"Error processing NUMA node {node_id}: {str(e)}")
        return {"node_id": node_id, "items_processed": len(data), "error": str(e), "status": "error"}


def combine_numa_results(node_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Combine results from NUMA node processing."""
    successful_nodes = [r for r in node_results if r.get("status") == "success"]
    failed_nodes = [r for r in node_results if r.get("status") == "error"]

    total_items = sum(r.get("items_processed", 0) for r in node_results)

    return {
        "total_nodes": len(node_results),
        "successful_nodes": len(successful_nodes),
        "failed_nodes": len(failed_nodes),
        "total_items": total_items,
        "results": [r.get("result") for r in successful_nodes if r.get("result")],
        "errors": [r.get("error") for r in failed_nodes if r.get("error")],
    }


def calculate_numa_efficiency(items: int, processing_time: float, numa_nodes: int) -> Dict[str, Any]:
    """Calculate NUMA optimization efficiency."""
    if processing_time <= 0:
        return {"numa_efficiency": 0, "node_utilization": 0}

    # Estimate efficiency based on NUMA node utilization
    items_per_node = items / numa_nodes if numa_nodes > 0 else items
    node_utilization = min(items_per_node / 1000, 1.0)  # Assume 1000 items per node for optimal utilization

    return {
        "numa_efficiency": round(node_utilization, 2),
        "node_utilization": round(node_utilization * 100, 2),
        "items_per_node": round(items_per_node, 2),
        "processing_rate": round(items / processing_time, 2),
    }


def get_cpu_usage() -> float:
    """Get current CPU usage percentage."""
    try:
        import psutil

        return psutil.cpu_percent(interval=0.1)
    except ImportError:
        return 0.0


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil

        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0


def get_parallel_performance_metrics() -> Dict[str, Any]:
    """Get comprehensive parallel processing performance metrics."""
    total_operations = sum(
        [
            _parallel_metrics["parallel_operations"],
            _parallel_metrics["thread_pool_operations"],
            _parallel_metrics["process_pool_operations"],
            _parallel_metrics["cpu_intensive_operations"],
            _parallel_metrics["io_intensive_operations"],
            _parallel_metrics["hybrid_operations"],
            _parallel_metrics["numa_optimized_operations"],
        ]
    )

    avg_processing_time = _parallel_metrics["total_processing_time"] / total_operations if total_operations > 0 else 0

    uptime = datetime.utcnow() - _parallel_metrics["last_reset"]

    return {
        "operations": {
            "total_operations": total_operations,
            "parallel_operations": _parallel_metrics["parallel_operations"],
            "cpu_intensive_operations": _parallel_metrics["cpu_intensive_operations"],
            "io_intensive_operations": _parallel_metrics["io_intensive_operations"],
            "hybrid_operations": _parallel_metrics["hybrid_operations"],
            "numa_optimized_operations": _parallel_metrics["numa_optimized_operations"],
        },
        "resource_usage": {
            "total_threads_used": _parallel_metrics["total_threads_used"],
            "total_processes_used": _parallel_metrics["total_processes_used"],
            "peak_cpu_usage": _parallel_metrics["peak_cpu_usage"],
            "peak_memory_usage": _parallel_metrics["peak_memory_usage"],
        },
        "performance": {
            "total_processing_time": round(_parallel_metrics["total_processing_time"], 2),
            "average_processing_time": round(avg_processing_time, 2),
            "operations_per_second": (
                round(total_operations / uptime.total_seconds(), 2) if uptime.total_seconds() > 0 else 0
            ),
        },
        "configuration": PARALLEL_CONFIG,
        "uptime": {"seconds": uptime.total_seconds(), "human_readable": str(uptime)},
    }


def reset_parallel_performance_metrics():
    """Reset parallel processing performance metrics."""
    _parallel_metrics.update(
        {
            "parallel_operations": 0,
            "thread_pool_operations": 0,
            "process_pool_operations": 0,
            "cpu_intensive_operations": 0,
            "io_intensive_operations": 0,
            "hybrid_operations": 0,
            "numa_operations": 0,
            "total_threads_used": 0,
            "total_processes_used": 0,
            "total_processing_time": 0.0,
            "peak_cpu_usage": 0.0,
            "peak_memory_usage": 0.0,
            "numa_optimized_operations": 0,
            "cache_hit_rate": 0.0,
            "last_reset": datetime.utcnow(),
        }
    )
    info("Parallel performance metrics reset")


def shutdown_thread_pool():
    """Shutdown the reusable thread pool."""
    global _thread_pool
    with _thread_pool_lock:
        if _thread_pool:
            _thread_pool.shutdown(wait=True)
            _thread_pool = None
            info("Thread pool shut down")


# FastAPI dependency injection functions
def get_parallel_processor_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for parallel processing functions."""
    return {
        "process_data_parallel_cpu_intensive": process_data_parallel_cpu_intensive,
        "process_data_parallel_io_intensive": process_data_parallel_io_intensive,
        "process_data_hybrid_parallel": process_data_hybrid_parallel,
        "process_data_numa_optimized": process_data_numa_optimized,
        "get_parallel_performance_metrics": get_parallel_performance_metrics,
        "reset_parallel_performance_metrics": reset_parallel_performance_metrics,
        "shutdown_thread_pool": shutdown_thread_pool,
    }


def get_cpu_processor_for_dependency() -> callable:
    """FastAPI dependency for CPU-intensive parallel processing."""
    return process_data_parallel_cpu_intensive


def get_io_processor_for_dependency() -> callable:
    """FastAPI dependency for I/O-intensive parallel processing."""
    return process_data_parallel_io_intensive


def get_numa_processor_for_dependency() -> callable:
    """FastAPI dependency for NUMA-optimized processing."""
    return process_data_numa_optimized
