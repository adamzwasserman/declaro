"""
Load testing scenarios for TableV2 performance validation.

This module provides comprehensive load testing with realistic scenarios
for 100+ concurrent users, 10k+ row tables, and complex formula chains.
"""

import asyncio
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from declaro_advise import error, info, success, warning
from declaro_tablix.performance.customization_optimizer import CustomizationContext, apply_customizations
from declaro_tablix.performance.formula_compiler import batch_execute_formulas, compile_formula, execute_formula
from declaro_tablix.performance.performance_monitor import get_performance_monitor


@dataclass
class LoadTestConfig:
    """Configuration for load testing scenarios."""

    # User simulation
    concurrent_users: int = 100
    test_duration_seconds: int = 300  # 5 minutes
    ramp_up_seconds: int = 60

    # Data configuration
    table_row_count: int = 10000
    table_column_count: int = 20
    formula_complexity: str = "medium"  # low, medium, high

    # Test scenarios
    formula_evaluation_ratio: float = 0.4  # 40% of operations are formula evaluations
    customization_ratio: float = 0.3  # 30% of operations are customizations
    data_retrieval_ratio: float = 0.3  # 30% of operations are data retrieval

    # Performance targets
    max_response_time_ms: float = 1000
    max_formula_time_ms: float = 5
    max_customization_time_per_1000_rows: float = 10
    target_throughput_ops_per_second: float = 100


@dataclass
class LoadTestResult:
    """Result of load testing scenario."""

    success: bool
    config: LoadTestConfig
    duration_seconds: float
    total_operations: int
    successful_operations: int
    failed_operations: int
    average_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    throughput_ops_per_second: float
    error_rate: float
    performance_targets_met: bool
    detailed_results: Dict[str, Any]


@dataclass
class UserSession:
    """Simulated user session."""

    user_id: str
    session_start_time: float
    operations_performed: int = 0
    total_response_time_ms: float = 0.0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class LoadTestRunner:
    """Load testing runner with multiple scenarios."""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.performance_monitor = get_performance_monitor()
        self.test_data = self._generate_test_data()
        self.formulas = self._generate_test_formulas()
        self.customizations = self._generate_test_customizations()
        self.results: List[Tuple[float, float, bool, str]] = []  # (timestamp, response_time, success, operation_type)
        self.user_sessions: Dict[str, UserSession] = {}
        self.test_start_time: Optional[float] = None

    def run_concurrent_users_test(self) -> LoadTestResult:
        """Run load test with concurrent users."""
        info(f"Starting concurrent users test with {self.config.concurrent_users} users")

        self.test_start_time = time.time()

        try:
            # Start performance monitoring
            self.performance_monitor.start_monitoring()

            # Use ThreadPoolExecutor for concurrent user simulation
            with ThreadPoolExecutor(max_workers=self.config.concurrent_users) as executor:
                # Submit user simulation tasks
                futures = []
                for i in range(self.config.concurrent_users):
                    user_id = f"user_{i:04d}"
                    future = executor.submit(self._simulate_user_session, user_id)
                    futures.append(future)

                # Wait for all users to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        error(f"User simulation failed: {str(e)}")

            # Calculate results
            result = self._calculate_load_test_results()

            if result.performance_targets_met:
                success(f"Load test completed successfully - targets met!")
            else:
                warning(f"Load test completed - performance targets not met")

            return result

        except Exception as e:
            error(f"Load test failed: {str(e)}")
            return LoadTestResult(
                success=False,
                config=self.config,
                duration_seconds=time.time() - self.test_start_time,
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                average_response_time_ms=0,
                p95_response_time_ms=0,
                p99_response_time_ms=0,
                throughput_ops_per_second=0,
                error_rate=1.0,
                performance_targets_met=False,
                detailed_results={"error": str(e)},
            )

    def run_large_table_test(self) -> LoadTestResult:
        """Run load test with large tables (10k+ rows)."""
        info(f"Starting large table test with {self.config.table_row_count} rows")

        self.test_start_time = time.time()

        try:
            # Generate large dataset
            large_data = self._generate_large_dataset()

            # Test customization performance on large data
            for i in range(10):  # 10 iterations
                context = CustomizationContext(
                    user_id=f"user_{i}",
                    table_name="large_table",
                    row_count=self.config.table_row_count,
                    column_count=self.config.table_column_count,
                    customizations=self.customizations,
                    data_snapshot_hash=f"snapshot_{i}",
                    timestamp=time.time(),
                )

                start_time = time.time()
                result = apply_customizations(large_data, context)
                response_time_ms = (time.time() - start_time) * 1000

                self.results.append((time.time(), response_time_ms, result.success, "large_table_customization"))

            # Calculate results
            result = self._calculate_load_test_results()

            if result.performance_targets_met:
                success(f"Large table test completed successfully")
            else:
                warning(f"Large table test completed - performance targets not met")

            return result

        except Exception as e:
            error(f"Large table test failed: {str(e)}")
            return LoadTestResult(
                success=False,
                config=self.config,
                duration_seconds=time.time() - self.test_start_time,
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                average_response_time_ms=0,
                p95_response_time_ms=0,
                p99_response_time_ms=0,
                throughput_ops_per_second=0,
                error_rate=1.0,
                performance_targets_met=False,
                detailed_results={"error": str(e)},
            )

    def run_complex_formula_test(self) -> LoadTestResult:
        """Run load test with complex formula chains."""
        info(f"Starting complex formula test with {len(self.formulas)} formulas")

        self.test_start_time = time.time()

        try:
            # Test formula compilation and execution
            for iteration in range(50):  # 50 iterations
                for formula_expression in self.formulas:
                    # Compile formula
                    start_time = time.time()
                    compile_result = compile_formula(
                        formula_expression, [f"col_{i}" for i in range(self.config.table_column_count)]
                    )
                    compile_time_ms = (time.time() - start_time) * 1000

                    self.results.append((time.time(), compile_time_ms, compile_result.success, "formula_compilation"))

                    if compile_result.success:
                        # Execute formula
                        start_time = time.time()
                        execute_result = execute_formula(
                            compile_result,
                            {f"col_{i}": random.randint(1, 100) for i in range(self.config.table_column_count)},
                        )
                        execute_time_ms = (time.time() - start_time) * 1000

                        self.results.append((time.time(), execute_time_ms, execute_result.success, "formula_execution"))

            # Calculate results
            result = self._calculate_load_test_results()

            if result.performance_targets_met:
                success(f"Complex formula test completed successfully")
            else:
                warning(f"Complex formula test completed - performance targets not met")

            return result

        except Exception as e:
            error(f"Complex formula test failed: {str(e)}")
            return LoadTestResult(
                success=False,
                config=self.config,
                duration_seconds=time.time() - self.test_start_time,
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                average_response_time_ms=0,
                p95_response_time_ms=0,
                p99_response_time_ms=0,
                throughput_ops_per_second=0,
                error_rate=1.0,
                performance_targets_met=False,
                detailed_results={"error": str(e)},
            )

    def run_stress_test(self) -> LoadTestResult:
        """Run comprehensive stress test combining all scenarios."""
        info(f"Starting comprehensive stress test")

        self.test_start_time = time.time()

        try:
            # Run multiple test scenarios in parallel
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(self._run_concurrent_operations),
                    executor.submit(self._run_large_data_operations),
                    executor.submit(self._run_complex_formula_operations),
                    executor.submit(self._run_cache_stress_operations),
                ]

                # Wait for all scenarios to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        error(f"Stress test scenario failed: {str(e)}")

            # Calculate results
            result = self._calculate_load_test_results()

            if result.performance_targets_met:
                success(f"Comprehensive stress test completed successfully")
            else:
                warning(f"Comprehensive stress test completed - performance targets not met")

            return result

        except Exception as e:
            error(f"Stress test failed: {str(e)}")
            return LoadTestResult(
                success=False,
                config=self.config,
                duration_seconds=time.time() - self.test_start_time,
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                average_response_time_ms=0,
                p95_response_time_ms=0,
                p99_response_time_ms=0,
                throughput_ops_per_second=0,
                error_rate=1.0,
                performance_targets_met=False,
                detailed_results={"error": str(e)},
            )

    def _simulate_user_session(self, user_id: str) -> None:
        """Simulate a user session with realistic operations."""
        session = UserSession(
            user_id=user_id,
            session_start_time=time.time(),
        )
        self.user_sessions[user_id] = session

        # Ramp up delay
        delay = random.uniform(0, self.config.ramp_up_seconds)
        time.sleep(delay)

        session_end_time = time.time() + self.config.test_duration_seconds

        while time.time() < session_end_time:
            try:
                # Choose operation type randomly
                operation_type = self._choose_operation_type()

                # Execute operation
                start_time = time.time()
                success_flag = self._execute_operation(operation_type, user_id)
                response_time_ms = (time.time() - start_time) * 1000

                # Record result
                self.results.append((time.time(), response_time_ms, success_flag, operation_type))

                # Update session metrics
                session.operations_performed += 1
                session.total_response_time_ms += response_time_ms

                if not success_flag:
                    session.errors.append(f"Operation {operation_type} failed")

                # Wait between operations
                time.sleep(random.uniform(0.1, 2.0))

            except Exception as e:
                session.errors.append(f"Operation error: {str(e)}")
                time.sleep(1.0)

    def _choose_operation_type(self) -> str:
        """Choose operation type based on configured ratios."""
        rand = random.random()

        if rand < self.config.formula_evaluation_ratio:
            return "formula_evaluation"
        elif rand < self.config.formula_evaluation_ratio + self.config.customization_ratio:
            return "customization"
        else:
            return "data_retrieval"

    def _execute_operation(self, operation_type: str, user_id: str) -> bool:
        """Execute a specific operation type."""
        try:
            if operation_type == "formula_evaluation":
                return self._execute_formula_evaluation(user_id)
            elif operation_type == "customization":
                return self._execute_customization(user_id)
            elif operation_type == "data_retrieval":
                return self._execute_data_retrieval(user_id)
            else:
                return False
        except Exception:
            return False

    def _execute_formula_evaluation(self, user_id: str) -> bool:
        """Execute formula evaluation operation."""
        formula_expression = random.choice(self.formulas)

        # Compile formula
        compile_result = compile_formula(formula_expression, [f"col_{i}" for i in range(self.config.table_column_count)])

        if not compile_result.success:
            return False

        # Execute formula
        execute_result = execute_formula(
            compile_result, {f"col_{i}": random.randint(1, 100) for i in range(self.config.table_column_count)}
        )

        return execute_result.success

    def _execute_customization(self, user_id: str) -> bool:
        """Execute customization operation."""
        context = CustomizationContext(
            user_id=user_id,
            table_name="test_table",
            row_count=random.randint(100, 1000),
            column_count=self.config.table_column_count,
            customizations=self.customizations,
            data_snapshot_hash=f"snapshot_{user_id}",
            timestamp=time.time(),
        )

        result = apply_customizations(self.test_data, context)
        return result.success

    def _execute_data_retrieval(self, user_id: str) -> bool:
        """Execute data retrieval operation."""
        # Simulate data retrieval
        time.sleep(random.uniform(0.01, 0.05))  # 10-50ms
        return True

    def _run_concurrent_operations(self) -> None:
        """Run concurrent operations stress test."""
        for _ in range(100):
            self._execute_operation("formula_evaluation", "stress_user")
            time.sleep(0.01)

    def _run_large_data_operations(self) -> None:
        """Run large data operations stress test."""
        large_data = self._generate_large_dataset()
        for _ in range(10):
            context = CustomizationContext(
                user_id="stress_user_large",
                table_name="stress_table",
                row_count=self.config.table_row_count,
                column_count=self.config.table_column_count,
                customizations=self.customizations,
                data_snapshot_hash="stress_snapshot",
                timestamp=time.time(),
            )
            apply_customizations(large_data, context)
            time.sleep(0.1)

    def _run_complex_formula_operations(self) -> None:
        """Run complex formula operations stress test."""
        complex_formulas = self._generate_complex_formulas()
        for formula in complex_formulas:
            compile_result = compile_formula(formula, [f"col_{i}" for i in range(self.config.table_column_count)])
            if compile_result.success:
                execute_formula(
                    compile_result, {f"col_{i}": random.randint(1, 100) for i in range(self.config.table_column_count)}
                )
            time.sleep(0.05)

    def _run_cache_stress_operations(self) -> None:
        """Run cache stress operations."""
        # Generate cache thrashing scenarios
        for _ in range(200):
            # Vary the formula slightly to test cache effectiveness
            base_formula = "{col_1} + {col_2} * {col_3}"
            modified_formula = f"{base_formula} + {random.randint(1, 10)}"

            compile_result = compile_formula(modified_formula, [f"col_{i}" for i in range(self.config.table_column_count)])

            if compile_result.success:
                execute_formula(
                    compile_result, {f"col_{i}": random.randint(1, 100) for i in range(self.config.table_column_count)}
                )
            time.sleep(0.01)

    def _generate_test_data(self) -> Dict[str, Any]:
        """Generate test data for load testing."""
        return {f"col_{i}": [random.randint(1, 100) for _ in range(100)] for i in range(self.config.table_column_count)}

    def _generate_large_dataset(self) -> Dict[str, Any]:
        """Generate large dataset for testing."""
        return {
            f"col_{i}": [random.randint(1, 100) for _ in range(self.config.table_row_count)]
            for i in range(self.config.table_column_count)
        }

    def _generate_test_formulas(self) -> List[str]:
        """Generate test formulas based on complexity level."""
        if self.config.formula_complexity == "low":
            return [
                "{col_1} + {col_2}",
                "{col_1} * {col_2}",
                "{col_1} - {col_2}",
                "{col_1} / {col_2}",
                "abs({col_1})",
            ]
        elif self.config.formula_complexity == "medium":
            return [
                "{col_1} + {col_2} * {col_3}",
                "round({col_1} / {col_2}, 2)",
                "max({col_1}, {col_2})",
                "min({col_1}, {col_2})",
                "({col_1} + {col_2}) / ({col_3} + {col_4})",
            ]
        else:  # high
            return [
                "round(({col_1} + {col_2}) * {col_3} / max({col_4}, {col_5}), 2)",
                "abs({col_1} - {col_2}) + round({col_3} * {col_4}, 1)",
                "max(min({col_1}, {col_2}), {col_3}) + {col_4}",
                "({col_1} + {col_2} + {col_3}) / 3",
                "round({col_1} * {col_2} + {col_3} * {col_4} - {col_5}, 2)",
            ]

    def _generate_complex_formulas(self) -> List[str]:
        """Generate complex formulas for stress testing."""
        return [
            "round(({col_1} + {col_2} + {col_3}) / 3 * {col_4} + max({col_5}, {col_6}), 2)",
            "abs({col_1} - {col_2}) + round({col_3} * {col_4} / max({col_5}, 1), 1)",
            "min(max({col_1}, {col_2}), {col_3}) + max(min({col_4}, {col_5}), {col_6})",
            "round({col_1} * {col_2} + {col_3} * {col_4} - {col_5} + {col_6}, 3)",
            "({col_1} + {col_2}) * ({col_3} + {col_4}) / ({col_5} + {col_6} + 1)",
        ]

    def _generate_test_customizations(self) -> Dict[str, Any]:
        """Generate test customizations."""
        return {
            "column_alias": {
                "col_1": "Revenue",
                "col_2": "Cost",
                "col_3": "Profit",
            },
            "column_filters": {
                "col_1": {"operator": "gt", "value": 50},
                "col_2": {"operator": "lt", "value": 100},
            },
            "formula_columns": {
                "calculated_1": "{col_1} + {col_2}",
                "calculated_2": "{col_1} * {col_2}",
            },
        }

    def _calculate_load_test_results(self) -> LoadTestResult:
        """Calculate comprehensive load test results."""
        if not self.results:
            return LoadTestResult(
                success=False,
                config=self.config,
                duration_seconds=0,
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                average_response_time_ms=0,
                p95_response_time_ms=0,
                p99_response_time_ms=0,
                throughput_ops_per_second=0,
                error_rate=1.0,
                performance_targets_met=False,
                detailed_results={},
            )

        # Calculate basic metrics
        total_operations = len(self.results)
        successful_operations = sum(1 for _, _, success, _ in self.results if success)
        failed_operations = total_operations - successful_operations

        response_times = [rt for _, rt, _, _ in self.results]
        response_times.sort()

        average_response_time_ms = sum(response_times) / len(response_times)
        p95_response_time_ms = response_times[int(0.95 * len(response_times))]
        p99_response_time_ms = response_times[int(0.99 * len(response_times))]

        duration_seconds = time.time() - self.test_start_time
        throughput_ops_per_second = total_operations / duration_seconds
        error_rate = failed_operations / total_operations

        # Check performance targets
        targets_met = (
            average_response_time_ms <= self.config.max_response_time_ms
            and p95_response_time_ms <= self.config.max_response_time_ms * 1.5
            and throughput_ops_per_second >= self.config.target_throughput_ops_per_second
            and error_rate <= 0.01  # 1% error rate
        )

        # Detailed results by operation type
        detailed_results = {}
        for operation_type in ["formula_evaluation", "customization", "data_retrieval"]:
            op_results = [(rt, success) for _, rt, success, op_type in self.results if op_type == operation_type]
            if op_results:
                op_response_times = [rt for rt, _ in op_results]
                op_success_rate = sum(1 for _, success in op_results if success) / len(op_results)

                detailed_results[operation_type] = {
                    "total_operations": len(op_results),
                    "average_response_time_ms": sum(op_response_times) / len(op_response_times),
                    "success_rate": op_success_rate,
                }

        return LoadTestResult(
            success=targets_met,
            config=self.config,
            duration_seconds=duration_seconds,
            total_operations=total_operations,
            successful_operations=successful_operations,
            failed_operations=failed_operations,
            average_response_time_ms=average_response_time_ms,
            p95_response_time_ms=p95_response_time_ms,
            p99_response_time_ms=p99_response_time_ms,
            throughput_ops_per_second=throughput_ops_per_second,
            error_rate=error_rate,
            performance_targets_met=targets_met,
            detailed_results=detailed_results,
        )


def run_performance_load_test(config: LoadTestConfig = None) -> LoadTestResult:
    """Run comprehensive performance load test."""
    if config is None:
        config = LoadTestConfig()

    runner = LoadTestRunner(config)
    return runner.run_concurrent_users_test()


def run_large_table_load_test(row_count: int = 10000) -> LoadTestResult:
    """Run load test specifically for large tables."""
    config = LoadTestConfig(table_row_count=row_count, concurrent_users=10)
    runner = LoadTestRunner(config)
    return runner.run_large_table_test()


def run_complex_formula_load_test() -> LoadTestResult:
    """Run load test specifically for complex formulas."""
    config = LoadTestConfig(formula_complexity="high", concurrent_users=20)
    runner = LoadTestRunner(config)
    return runner.run_complex_formula_test()


def run_comprehensive_stress_test() -> LoadTestResult:
    """Run comprehensive stress test with all scenarios."""
    config = LoadTestConfig(
        concurrent_users=150,
        test_duration_seconds=600,  # 10 minutes
        table_row_count=15000,
        formula_complexity="high",
    )
    runner = LoadTestRunner(config)
    return runner.run_stress_test()


# Export all functions and classes
__all__ = [
    "LoadTestConfig",
    "LoadTestResult",
    "LoadTestRunner",
    "run_performance_load_test",
    "run_large_table_load_test",
    "run_complex_formula_load_test",
    "run_comprehensive_stress_test",
]
