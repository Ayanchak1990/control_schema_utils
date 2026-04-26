"""
Data quality runner for quper_control_schema_utils.

Executes DQ rules against DataFrames and logs results to the
dq_check_results table.
"""

import logging
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, List, Tuple

from pyspark.sql.functions import col, current_timestamp, expr, lit, struct, to_json
from pyspark.sql.functions import expr as spark_expr

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.exceptions import DQFailureError
from quper_control_schema_utils.models import (
    DQAction,
    DQResult,
    DQRule,
    DQStatus,
    Severity,
    TableName,
)

logger = logging.getLogger(__name__)


def run_dq_checks(
    spark: Any,
    run_id: str,
    pipeline_id: str,
    object_id: str,
    df: Any,
    rules: List[DQRule],
    environment: str,
) -> List[DQResult]:
    """
    Run all DQ rules against the given DataFrame.

    Rule evaluation uses DataFrame operations — never spark.sql() — so that
    expressions like "col IS NOT NULL" work correctly against the in-memory
    DataFrame rather than being parsed as standalone SQL.

    Rule types:
      NOT_NULL   — df.filter(col(column_name).isNull())
      RANGE      — df.filter(~expr(rule_expression))
      ROW_COUNT  — df.count() compared against int(expected_value)

    Any exception during rule evaluation propagates immediately — a rule that
    cannot be evaluated is a pipeline error, not a skippable warning.

    After all rules: if any ERROR severity rule FAILED, raise DQFailureError.

    Args:
        spark:       Active SparkSession.
        run_id:      Current run identifier.
        pipeline_id: Pipeline identifier.
        object_id:   Source object identifier.
        df:          PySpark DataFrame to validate.
        rules:       List of DQRule dataclass instances.
        environment: Deployment environment.

    Returns:
        List of DQResult dataclass instances.

    Raises:
        DQFailureError: If any ERROR severity rule fails or an unknown rule
                        type is encountered.
    """
    if not rules:
        logger.info(f"[pipeline={pipeline_id}, object={object_id}] No DQ rules to run")
        return []

    logger.info(f"[pipeline={pipeline_id}, object={object_id}] Running {len(rules)} DQ rule(s)")

    now = datetime.now(timezone.utc)
    results: List[DQResult] = []

    total_rows = df.count()

    for rule in rules:
        result_id = str(uuid.uuid4())

        if rule.rule_type == "NOT_NULL":
            failed_count = df.filter(col(rule.column_name).isNull()).count()
            passed_count = total_rows - failed_count
            rows_checked = total_rows

        elif rule.rule_type == "RANGE":
            failed_count = df.filter(~expr(rule.rule_expression)).count()
            passed_count = total_rows - failed_count
            rows_checked = total_rows

        elif rule.rule_type == "ROW_COUNT":
            actual_count = total_rows
            threshold = int(rule.expected_value)
            if actual_count >= threshold:
                failed_count = 0
                passed_count = actual_count
            else:
                failed_count = actual_count
                passed_count = 0
            rows_checked = actual_count

        else:
            raise DQFailureError(
                f"Unknown rule_type '{rule.rule_type}' for rule '{rule.rule_name}' "
                f"on object={object_id} — cannot evaluate"
            )

        pass_rate = (passed_count / rows_checked * 100.0) if rows_checked > 0 else 100.0

        if failed_count == 0:
            status = DQStatus.PASSED
            action_taken = DQAction.CONTINUED
        elif rule.severity == Severity.ERROR:
            status = DQStatus.FAILED
            action_taken = DQAction.HALTED
        else:
            status = DQStatus.WARNING
            action_taken = DQAction.CONTINUED

        result = DQResult(
            result_id=result_id,
            run_id=run_id,
            object_id=object_id,
            rule_id=rule.rule_id,
            pipeline_id=pipeline_id,
            rule_name=rule.rule_name,
            rule_type=rule.rule_type,
            column_name=rule.column_name,
            rows_checked=rows_checked,
            rows_passed=passed_count,
            rows_failed=failed_count,
            pass_rate=round(pass_rate, 2),
            status=status,
            action_taken=action_taken,
            checked_at=now,
            environment=environment,
        )
        results.append(result)
        logger.info(
            f"[pipeline={pipeline_id}, object={object_id}] "
            f"DQ rule '{rule.rule_name}': {status} (pass_rate={pass_rate:.1f}%)"
        )

    error_failures = [
        r for r in results
        if r.status == DQStatus.FAILED and r.action_taken == DQAction.HALTED
    ]
    if error_failures:
        failed_names = ", ".join(r.rule_name for r in error_failures)
        logger.error(f"[pipeline={pipeline_id}, object={object_id}] DQ check failed: {failed_names}")
        raise DQFailureError(
            f"DQ check failed for object={object_id}: {failed_names}"
        )

    logger.info(f"[pipeline={pipeline_id}, object={object_id}] All {len(rules)} DQ rule(s) passed")
    return results


def log_dq_results(
    spark: Any,
    catalog: str,
    control_schema: str,
    results: List[DQResult],
) -> None:
    """
    Insert all DQResult rows into dq_check_results.

    This function must never raise — on failure it logs a WARNING and returns.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        results:        List of DQResult dataclass instances.

    Returns:
        None.
    """
    try:
        if not results:
            return

        table = f"{catalog}.{control_schema}.{TableName.DQ_CHECK_RESULTS}"
        first = results[0]
        logger.info(f"[pipeline={first.pipeline_id}, object={first.object_id}] Logging {len(results)} DQ result(s) to {table}")

        values = []
        for r in results:
            col_val = f"'{_escape_sql(r.column_name)}'" if r.column_name else "NULL"
            rows_checked = r.rows_checked if r.rows_checked is not None else "NULL"
            rows_passed = r.rows_passed if r.rows_passed is not None else "NULL"
            rows_failed = r.rows_failed if r.rows_failed is not None else "NULL"
            pass_rate = r.pass_rate if r.pass_rate is not None else "NULL"

            values.append(
                f"('{_escape_sql(r.result_id)}', '{_escape_sql(r.run_id)}', "
                f"'{_escape_sql(r.object_id)}', '{_escape_sql(r.rule_id)}', "
                f"'{_escape_sql(r.pipeline_id)}', '{_escape_sql(r.rule_name)}', "
                f"'{_escape_sql(r.rule_type)}', {col_val}, "
                f"{rows_checked}, {rows_passed}, {rows_failed}, {pass_rate}, "
                f"'{_escape_sql(r.status)}', '{_escape_sql(r.action_taken)}', "
                f"current_timestamp(), '{_escape_sql(r.environment)}')"
            )

        values_str = ",\n".join(values)
        query = f"""
            INSERT INTO {table}
            (result_id, run_id, object_id, rule_id, pipeline_id,
             rule_name, rule_type, column_name, rows_checked,
             rows_passed, rows_failed, pass_rate, status,
             action_taken, checked_at, environment)
            VALUES
            {values_str}
        """
        spark.sql(query)
        logger.info(f"[pipeline={first.pipeline_id}, object={first.object_id}] Logged {len(results)} DQ result(s)")

    except Exception as e:
        context = f"pipeline={results[0].pipeline_id}, object={results[0].object_id}" if results else "unknown"
        logger.warning(f"[{context}] Failed to log DQ results: {e}\n{traceback.format_exc()}")


def split_and_quarantine(
    spark: Any,
    catalog: str,
    control_schema: str,
    run_id: str,
    pipeline_id: str,
    object_id: str,
    source_object: str,
    df: Any,
    rules: List[DQRule],
    dq_results: List[DQResult],
    environment: str,
) -> Tuple[Any, int]:
    """
    Split df into clean records and quarantine records based on WARN-severity DQ results.

    For each WARNING-severity rule that produced row-level failures (NOT_NULL, RANGE),
    failing rows are written to the dq_quarantine table with rejection metadata attached.
    The returned DataFrame contains only records that passed all WARNING rules, and is
    safe to pass directly to the merge writer.

    ROW_COUNT rules are aggregate checks — they have no individual rows to quarantine
    and are skipped by this function.

    ERROR-severity rules are not processed here — they halt the pipeline via
    DQFailureError before this function is reached.

    This function must never raise: a quarantine write failure is logged as a WARNING
    and the clean DataFrame is returned unchanged so the main pipeline is not blocked.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        run_id:         Current run identifier.
        pipeline_id:    Pipeline identifier.
        object_id:      Source object identifier.
        source_object:  Source object name (e.g. "yellow_tripdata").
        df:             Enriched DataFrame (post-metadata injection).
        rules:          DQRule list evaluated this run.
        dq_results:     DQResult list from run_dq_checks() for this run.
        environment:    Deployment environment.

    Returns:
        Tuple of (clean_df, rows_rejected):
            clean_df      — DataFrame with all WARNING-rule violations removed.
            rows_rejected — Count of unique rows removed from df.
    """
    # Index results by rule_id for O(1) lookup
    result_by_rule_id = {r.rule_id: r for r in dq_results}

    quarantine_parts: List[Any] = []
    clean_df = df

    for rule in rules:
        result = result_by_rule_id.get(rule.rule_id)
        if result is None or result.rows_failed == 0:
            continue
        if rule.severity != Severity.WARNING:
            continue  # ERROR rules already halted the pipeline before this point
        if rule.rule_type == "ROW_COUNT":
            continue  # aggregate check — no individual rows to quarantine

        if rule.rule_type == "NOT_NULL":
            fail_filter = col(rule.column_name).isNull()
        elif rule.rule_type == "RANGE":
            fail_filter = ~expr(rule.rule_expression)
        else:
            logger.warning(
                f"[pipeline={pipeline_id}, object={object_id}] "
                f"Cannot quarantine rows for unknown rule_type '{rule.rule_type}' "
                f"on rule '{rule.rule_name}' — skipping"
            )
            continue

        quarantine_part = (
            df.filter(fail_filter)
            .select(
                spark_expr("uuid()").alias("quarantine_id"),
                lit(run_id).alias("run_id"),
                lit(pipeline_id).alias("pipeline_id"),
                lit(object_id).alias("object_id"),
                lit(source_object).alias("source_table"),
                lit(rule.rule_id).alias("rule_id"),
                lit(rule.rule_name).alias("rule_name"),
                lit(f"{rule.rule_type} violation: {rule.rule_name}").alias("rejection_reason"),
                current_timestamp().alias("rejected_at"),
                to_json(struct([col(c) for c in df.columns])).alias("raw_record"),
                lit(environment).alias("environment"),
            )
        )
        quarantine_parts.append(quarantine_part)

        # Remove failing rows from the clean path
        clean_df = clean_df.filter(~fail_filter)

    if not quarantine_parts:
        return df, 0

    rows_rejected = df.count() - clean_df.count()

    try:
        from functools import reduce
        quarantine_df = reduce(lambda a, b: a.unionAll(b), quarantine_parts)

        quarantine_table = f"{catalog}.{control_schema}.{TableName.DQ_QUARANTINE}"
        if not spark.catalog.tableExists(quarantine_table):
            (
                quarantine_df.limit(0)
                .write.format("delta")
                .option("delta.autoOptimize.optimizeWrite", "true")
                .option("delta.autoOptimize.autoCompact", "true")
                .mode("ignore")
                .saveAsTable(quarantine_table)
            )

        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

        logger.info(
            f"[pipeline={pipeline_id}, object={object_id}] "
            f"Quarantined {rows_rejected} row(s) to {quarantine_table}"
        )

    except Exception as e:
        logger.warning(
            f"[pipeline={pipeline_id}, object={object_id}] "
            f"Failed to write quarantine records — pipeline continues: {e}\n{traceback.format_exc()}"
        )

    return clean_df, rows_rejected
