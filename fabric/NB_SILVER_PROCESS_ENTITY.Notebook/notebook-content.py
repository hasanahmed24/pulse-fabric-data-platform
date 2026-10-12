# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # PULSE Silver Processing Framework
# 
# **Notebook:** `NB_SILVER_PROCESS_ENTITY`
# 
# This notebook implements the metadata-driven transformation of Bronze data into the Silver layer.
# 
# ### Responsibilities
# - Resolve environment-specific Bronze and Silver Lakehouses dynamically.
# - Read the configured Bronze entity.
# - Apply incremental filtering where required.
# - Standardize and cleanse source data.
# - Apply data-quality rules.
# - Deduplicate records.
# - Quarantine invalid records.
# - Write valid data to Silver using the configured write strategy.
# - Return processing metrics to the orchestration pipeline.
# 
# The same notebook is designed to run across DEV, UAT, and PROD without hardcoded workspace or Lakehouse identifiers.

# MARKDOWN ********************

# ## 1. Runtime Parameters
# 
# The notebook receives entity-specific configuration from the Silver orchestration pipeline.
# 
# During interactive development, default values are populated for the `Person` entity.  
# When executed from the pipeline, these values are overridden dynamically from `ctl.SilverConfig`, `ctl.SilverWatermark`, and the Variable Library.

# PARAMETERS CELL ********************

# Runtime Parameters
# This cell must remain marked as a Parameter Cell.
RunId = "MANUAL_TEST_MERCHANT"
EnvironmentName = "DEV"

EntityName = "Merchant"

SourceSchema = "dbo"
SourceTable = "brz_merchant"

TargetSchema = "silver"
TargetTable = "merchant"

LoadType = "FULL"
WriteStrategy = "OVERWRITE"

WatermarkColumn = ""
StartWatermark = "0"
EndWatermark = "0"

MergeKeyColumns = "MerchantRecordID"
DedupKeyColumns = "MerchantRecordID"

TransformProfile = "MERCHANT"

QuarantineSchema = "quarantine"
QuarantineTable = "merchant"

MaxRejectPct = 1.0

DQRulesJson = "[]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

import json
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("PULSE_SILVER")

logger.info(
    f"Starting Silver processing | "
    f"RunId={RunId} | "
    f"Entity={EntityName} | "
    f"Environment={EnvironmentName}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Resolve Environment-Specific Lakehouse References
# 
# Bronze and Silver Lakehouse references are retrieved from `VL_PULSE_CONFIG`.
# 
# Because the variables are stored as Fabric Item References, each value contains:
# - `workspaceId`
# - `itemId`
# 
# The active Variable Library value set determines whether DEV, UAT, or PROD resources are returned. No environment-specific IDs are hardcoded in the notebook.

# CELL ********************

# Resolve environment-specific Lakehouse references

bronze_ref = notebookutils.variableLibrary.get(
    "$(/ ** /VL_PULSE_CONFIG/BronzeLakehouse)".replace(" ", "")
)

silver_ref = notebookutils.variableLibrary.get(
    "$(/ ** /VL_PULSE_CONFIG/SilverLakehouse)".replace(" ", "")
)

# In this Fabric runtime the values are already strings
bronze_workspace_id = bronze_ref.get("workspaceId")
bronze_lakehouse_id = bronze_ref.get("itemId")

silver_workspace_id = silver_ref.get("workspaceId")
silver_lakehouse_id = silver_ref.get("itemId")

print("Environment:", EnvironmentName)

print("Bronze Workspace:", bronze_workspace_id)
print("Bronze Lakehouse:", bronze_lakehouse_id)

print("Silver Workspace:", silver_workspace_id)
print("Silver Lakehouse:", silver_lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(type(bronze_ref))
print(bronze_ref)

print(type(silver_ref))
print(silver_ref)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Read Bronze Entity Dynamically
# 
# The Bronze source path is constructed dynamically from:
# 
# - Active environment's Bronze workspace ID
# - Active environment's Bronze Lakehouse ID
# - `SourceSchema`
# - `SourceTable`
# 
# This enables the same notebook to process Person, Merchant, and TransactionDetail without hardcoding table paths.

# CELL ********************

# Build dynamic Bronze OneLake path

bronze_path = (
    f"abfss://{bronze_workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{bronze_lakehouse_id}/"
    f"Tables/{SourceSchema}/{SourceTable}"
)

print(f"Entity       : {EntityName}")
print(f"Source       : {SourceSchema}.{SourceTable}")
print(f"Bronze Path  : {bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read configured Bronze Delta table

bronze_df = (
    spark.read
         .format("delta")
         .load(bronze_path)
)

source_row_count = bronze_df.count()

print(f"Rows read    : {source_row_count:,}")

bronze_df.printSchema()

display(bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Manual high-watermark capture for interactive notebook testing only.
# The orchestration pipeline will provide EndWatermark later.
"""
if LoadType.strip().upper() == "INCREMENTAL":

    current_high_watermark = (
        bronze_df
        .agg(F.max(F.col(WatermarkColumn)).alias("MaxWatermark"))
        .first()["MaxWatermark"]
    )

    EndWatermark = str(current_high_watermark)

    print(f"Start Watermark : {StartWatermark}")
    print(f"End Watermark   : {EndWatermark}")

else:

    print(
        f"High watermark not required for "
        f"{EntityName} because LoadType = {LoadType}"
    )
    """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## 4. Apply Load Strategy
# 
# The configured `LoadType` determines which Bronze records are processed.
# 
# - **FULL** — process the entire Bronze table.
# - **INCREMENTAL** — process only Bronze records that have not yet been
#   successfully processed by Silver.
# 
# For incremental processing:
# 
# `StartWatermark < WatermarkColumn <= EndWatermark`
# 
# `StartWatermark` is the last successfully committed Silver watermark.
# 
# `EndWatermark` is the maximum available watermark in Bronze when the
# Silver batch begins. It freezes the processing boundary for the current
# run, enabling deterministic reconciliation and safe retry.
# 
# In the PULSE orchestration, Bronze is expected to complete before Silver
# starts. The frozen end watermark also protects the framework if concurrent
# or overlapping Bronze ingestion is introduced in the future.

# CELL ********************

# Apply FULL or INCREMENTAL processing boundary

load_type = LoadType.strip().upper()

if load_type == "FULL":

    batch_df = bronze_df

    print("Load Type       : FULL")
    print(f"Entity          : {EntityName}")
    print("Processing      : Complete Bronze table")


elif load_type == "INCREMENTAL":

    # Validate required incremental parameters
    if not WatermarkColumn:
        raise ValueError(
            f"WatermarkColumn is required for incremental entity: {EntityName}"
        )

    if StartWatermark in (None, ""):
        raise ValueError(
            f"StartWatermark is required for incremental entity: {EntityName}"
        )

    if EndWatermark in (None, ""):
        raise ValueError(
            f"EndWatermark is required for incremental entity: {EntityName}"
        )

    start_watermark = int(StartWatermark)
    end_watermark = int(EndWatermark)

    # Basic boundary validation
    if end_watermark < start_watermark:
        raise ValueError(
            f"Invalid watermark range for {EntityName}: "
            f"EndWatermark ({end_watermark}) is less than "
            f"StartWatermark ({start_watermark})"
        )

    batch_df = bronze_df.filter(
        (F.col(WatermarkColumn) > F.lit(start_watermark)) &
        (F.col(WatermarkColumn) <= F.lit(end_watermark))
    )

    print("Load Type       : INCREMENTAL")
    print(f"Entity          : {EntityName}")
    print(f"Watermark Column: {WatermarkColumn}")
    print(f"Start Watermark : {start_watermark}")
    print(f"End Watermark   : {end_watermark}")


else:

    raise ValueError(
        f"Unsupported LoadType '{LoadType}' for entity: {EntityName}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4.1 Materialize Processing Batch
# 
# The filtered DataFrame represents the exact set of Bronze records selected for the current Silver execution.
# 
# From this point onward, cleansing, transformation, deduplication, data-quality validation, and Silver writes operate on `batch_df`, not the complete Bronze DataFrame.

# CELL ********************


# Materialize selected Silver processing batch

batch_row_count = batch_df.count()

print(f"Batch rows      : {batch_row_count:,}")


# Skip processing when an incremental run has no new records

if load_type == "INCREMENTAL" and batch_row_count == 0:

    print(
        f"No new records found for {EntityName}. "
        "Existing Silver table will remain unchanged."
    )

    notebookutils.notebook.exit(
        json.dumps({
            "EntityName": EntityName,
            "Status": "NO_NEW_DATA",
            "RowsProcessed": 0,
            "StartWatermark": StartWatermark,
            "EndWatermark": EndWatermark
        })
    )


display(batch_df.limit(10))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Data Cleansing and Standardization
# 
# The selected Silver batch is standardized before applying data-quality rules.
# 
# Processing is divided into two levels:
# 
# 1. **Generic cleansing**
#    - Trim leading and trailing whitespace from all string columns.
# 
# 2. **Entity-specific transformation**
#    - Person: standardize categorical attributes.
#    - Merchant: standardize merchant classification attributes.
#    - TransactionDetail: standardize currencies/payment format and derive transaction date.
# 
# Business identifiers such as `PersonID`, `MerchantID`, account numbers, and `TransactionID` are not modified other than whitespace trimming.

# CELL ********************

# Generic string cleansing

def trim_string_columns(df):
    """
    Trim leading and trailing whitespace from every string column.
    """

    string_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for column_name in string_columns:
        df = df.withColumn(
            column_name,
            F.trim(F.col(column_name))
        )

    return df


clean_df = trim_string_columns(batch_df)

print(f"Generic cleansing completed for: {EntityName}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5.1 Entity-Specific Transformation Profiles
# 
# Entity-specific transformation logic is implemented as reusable PySpark functions.
# 
# `TransformProfile` from `ctl.SilverConfig` determines which transformation function is executed.
# 
# This keeps orchestration metadata-driven while keeping transformation code version-controlled and maintainable.

# CELL ********************

# Entity-specific transformation functions

def transform_person(df):

    return (
        df
        .withColumn(
            "PersonGender",
            F.lower(F.col("PersonGender"))
        )
        .withColumn(
            "PersonMaritalStatus",
            F.lower(F.col("PersonMaritalStatus"))
        )
        .withColumn(
            "PersonEducation",
            F.lower(F.col("PersonEducation"))
        )
        .withColumn(
            "PersonOccupation",
            F.lower(F.col("PersonOccupation"))
        )
    )


def transform_merchant(df):

    return (
        df
        .withColumn(
            "MerchantType",
            F.lower(F.col("MerchantType"))
        )
        .withColumn(
            "Industry",
            F.lower(F.col("Industry"))
        )
        .withColumn(
            "OperatingStatus",
            F.lower(F.col("OperatingStatus"))
        )
    )


def transform_transaction(df):

    return (
        df
        .withColumn(
            "ReceivingCurrency",
            F.upper(F.col("ReceivingCurrency"))
        )
        .withColumn(
            "PaymentCurrency",
            F.upper(F.col("PaymentCurrency"))
        )
        .withColumn(
            "PaymentFormat",
            F.lower(F.col("PaymentFormat"))
        )
        .withColumn(
            "TransactionDate",
            F.to_date(F.col("TransactionTimestamp"))
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformation profile dispatcher

TRANSFORMERS = {
    "PERSON": transform_person,
    "MERCHANT": transform_merchant,
    "TRANSACTION": transform_transaction
}

transform_profile = TransformProfile.strip().upper()

if transform_profile not in TRANSFORMERS:
    raise ValueError(
        f"Unsupported TransformProfile '{TransformProfile}' "
        f"for entity '{EntityName}'"
    )

silver_df = TRANSFORMERS[transform_profile](clean_df)

print(f"Transformation Profile : {transform_profile}")
print(f"Entity                 : {EntityName}")
print("Transformation Status  : SUCCESS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5.2 Add Silver Technical Lineage Columns
# 
# Operational metadata is added to each Silver record for traceability.
# 
# - `_silver_run_id` identifies the pipeline/notebook execution.
# - `_silver_processed_at_utc` records when the row was processed.
# - `_record_hash` provides a deterministic hash of the business data and can support future change-detection or reconciliation logic.

# CELL ********************

# Add Silver operational metadata

silver_df = (
    silver_df
    .withColumn(
        "_silver_run_id",
        F.lit(RunId)
    )
    .withColumn(
        "_silver_processed_at_utc",
        F.current_timestamp()
    )
)

# Business columns used for deterministic record hash
hash_columns = [
    column_name
    for column_name in silver_df.columns
    if not column_name.startswith("_silver_")
]

silver_df = silver_df.withColumn(
    "_record_hash",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(column_name).cast("string"),
                    F.lit("")
                )
                for column_name in hash_columns
            ]
        ),
        256
    )
)

print("Silver lineage columns added successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Rows after transformation: {silver_df.count():,}")

silver_df.printSchema()

display(silver_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Technical Deduplication
# 
# Before applying data-quality rules, the Silver batch is deduplicated using the key defined in `ctl.SilverConfig`.
# 
# The deduplication key is passed to the notebook through the `DedupKeyColumns` parameter.
# 
# Current configuration:
# 
# - Person → `PersonID`
# - Merchant → `MerchantRecordID`
# - TransactionDetail → `TransactionID`
# 
# Deduplication is performed only on the records selected for the current processing batch.
# 
# For incremental entities, retry protection across different runs is additionally handled by the Silver write strategy (`MERGE`) using the configured merge key.

# CELL ********************

# Resolve metadata-driven deduplication keys

dedup_keys = [
    column_name.strip()
    for column_name in DedupKeyColumns.split(",")
    if column_name.strip()
]

if not dedup_keys:
    raise ValueError(
        f"No DedupKeyColumns configured for entity: {EntityName}"
    )

# Validate that all configured keys exist in the DataFrame
missing_keys = [
    column_name
    for column_name in dedup_keys
    if column_name not in silver_df.columns
]

if missing_keys:
    raise ValueError(
        f"Configured deduplication columns do not exist "
        f"for {EntityName}: {missing_keys}"
    )

print(f"Entity          : {EntityName}")
print(f"Deduplication Key(s): {dedup_keys}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove duplicate records based on configured technical/business key

dedup_df = silver_df.dropDuplicates(dedup_keys)

dedup_row_count = dedup_df.count()

duplicate_rows_removed = batch_row_count - dedup_row_count

print(f"Input Rows             : {batch_row_count:,}")
print(f"Rows After Deduplication: {dedup_row_count:,}")
print(f"Duplicates Removed     : {duplicate_rows_removed:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Metadata-Driven Data Quality Validation
# 
# Data-quality rules are maintained in `ctl.SilverDQRule` rather than hardcoded throughout the notebook.
# 
# During pipeline execution, the active rules for the current entity will be retrieved by the orchestration pipeline and passed to this notebook as JSON.
# 
# Rules are classified by action:
# 
# - **QUARANTINE** — invalid rows are removed from the valid Silver dataset and written to the quarantine area.
# - **WARN** — the row remains valid, but the data-quality issue is recorded.
# - **FAIL** — a critical rule violation can fail the entity processing.
# 
# Only rules configured with `ExecutionTier = INLINE` are evaluated in the critical Silver processing path.

# MARKDOWN ********************

# ### 7.1 Parse Active Inline DQ Rules
# 
# The JSON rule payload is converted into Python objects.
# 
# Only active `INLINE` rules are used by the Silver transformation notebook. More expensive profiling rules are excluded from the critical ETL path.

# CELL ********************

# Parse metadata-driven DQ rules

dq_rules = json.loads(DQRulesJson)

inline_rules = [
    rule
    for rule in dq_rules
    if int(rule.get("IsActive", 1)) == 1
    and rule.get("ExecutionTier", "INLINE").upper() == "INLINE"
]

quarantine_rules = [
    rule for rule in inline_rules
    if rule["Action"].upper() == "QUARANTINE"
]

warning_rules = [
    rule for rule in inline_rules
    if rule["Action"].upper() == "WARN"
]

fail_rules = [
    rule for rule in inline_rules
    if rule["Action"].upper() == "FAIL"
]

print(f"Entity           : {EntityName}")
print(f"Active DQ Rules  : {len(inline_rules)}")
print(f"Quarantine Rules : {len(quarantine_rules)}")
print(f"Warning Rules    : {len(warning_rules)}")
print(f"Fail Rules       : {len(fail_rules)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7.2 Evaluate DQ Rules
# 
# Active rule expressions are evaluated against the deduplicated Silver batch.
# 
# Rules are converted into Spark SQL expressions and evaluated as part of the same logical transformation plan rather than executing one independent ETL scan per rule.
# 
# Each row receives:
# 
# - `_dq_failed_rules` — quarantine rule codes violated by the row.
# - `_dq_warning_rules` — warning rule codes violated by the row.

# CELL ********************

# Build row-level DQ evaluation expressions

quarantine_failure_expressions = [
    F.when(
        ~F.expr(rule["RuleExpression"]),
        F.lit(rule["RuleCode"])
    )
    for rule in quarantine_rules
]

warning_failure_expressions = [
    F.when(
        ~F.expr(rule["RuleExpression"]),
        F.lit(rule["RuleCode"])
    )
    for rule in warning_rules
]


# Handle case where an entity has no rules of a particular type
if quarantine_failure_expressions:
    quarantine_array = F.array(*quarantine_failure_expressions)
else:
    quarantine_array = F.array().cast("array<string>")


if warning_failure_expressions:
    warning_array = F.array(*warning_failure_expressions)
else:
    warning_array = F.array().cast("array<string>")


dq_df = (
    dedup_df

    .withColumn(
        "_dq_failed_rules",
        F.filter(
            quarantine_array,
            lambda x: x.isNotNull()
        )
    )

    .withColumn(
        "_dq_warning_rules",
        F.filter(
            warning_array,
            lambda x: x.isNotNull()
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7.3 Split Valid and Quarantined Records
# 
# Rows that fail one or more `QUARANTINE` rules are separated from the valid Silver dataset.
# 
# Warning-only records remain in the valid dataset.
# 
# Quarantined rows retain their failed rule codes and receive rejection metadata for troubleshooting and remediation.

# CELL ********************

# Split valid and invalid records

valid_df = (
    dq_df
    .filter(
        F.size(F.col("_dq_failed_rules")) == 0
    )
)

quarantine_df = (
    dq_df
    .filter(
        F.size(F.col("_dq_failed_rules")) > 0
    )
    .withColumn(
        "_dq_reason",
        F.concat_ws(
            "; ",
            F.col("_dq_failed_rules")
        )
    )
    .withColumn(
        "_rejected_at_utc",
        F.current_timestamp()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7.4 DQ Validation Metrics
# 
# The row counts below confirm how many records passed validation, were quarantined, or contain warning-level quality issues.

# CELL ********************

valid_row_count = valid_df.count()

quarantine_row_count = quarantine_df.count()

warning_row_count = (
    dq_df
    .filter(
        F.size(F.col("_dq_warning_rules")) > 0
    )
    .count()
)

reject_pct = (
    (quarantine_row_count / dedup_row_count) * 100
    if dedup_row_count > 0
    else 0.0
)

print(f"Input After Deduplication : {dedup_row_count:,}")
print(f"Valid Rows                : {valid_row_count:,}")
print(f"Quarantined Rows          : {quarantine_row_count:,}")
print(f"Warning Rows              : {warning_row_count:,}")
print(f"Reject Percentage         : {reject_pct:.4f}%")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display warning records dynamically

if warning_row_count > 0:

    warning_preview_columns = [
        column_name
        for column_name in dedup_keys
        if column_name in dq_df.columns
    ]

    warning_preview_columns.append("_dq_warning_rules")

    display(
        dq_df
        .filter(
            F.size(F.col("_dq_warning_rules")) > 0
        )
        .select(*warning_preview_columns)
        .limit(20)
    )

else:

    print(f"No warning records found for {EntityName}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Reject Threshold Validation
# 
# Before writing valid records to Silver, the notebook checks whether the percentage of quarantined records exceeds the configured tolerance.
# 
# `MaxRejectPct` is maintained in `ctl.SilverConfig`.
# 
# If the rejection percentage exceeds the configured threshold, processing fails and the Silver target is not updated.

# CELL ********************

# Validate reject percentage against configured threshold

max_reject_pct = float(MaxRejectPct)

print(f"Reject Percentage     : {reject_pct:.4f}%")
print(f"Maximum Allowed       : {max_reject_pct:.4f}%")

if reject_pct > max_reject_pct:
    raise ValueError(
        f"DQ reject threshold exceeded for {EntityName}. "
        f"RejectPct={reject_pct:.4f}% "
        f"MaxAllowed={max_reject_pct:.4f}%"
    )

print("DQ Threshold Status   : PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Write Valid Records to Silver
# 
# Records that successfully pass quarantine-level DQ validation are written to the configured Silver table.
# 
# The write behavior is controlled by `WriteStrategy` from `ctl.SilverConfig`:
# 
# - **OVERWRITE** — replace the complete Silver table.
# - **MERGE** — upsert records using the configured merge key.
# - **APPEND** — append new records without updating existing rows.
# 
# Current configuration:
# 
# - Person → OVERWRITE
# - Merchant → OVERWRITE
# - TransactionDetail → MERGE

# CELL ********************

# Build dynamic Silver target path

silver_target_path = (
    f"abfss://{silver_workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{silver_lakehouse_id}/"
    f"Tables/{TargetSchema}/{TargetTable}"
)

print(f"Silver Target : {TargetSchema}.{TargetTable}")
print(f"Target Path   : {silver_target_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare valid records for Silver write
# Convert DQ warning array to SQL-compatible string

valid_output_df = (
    valid_df
    .drop("_dq_failed_rules")
    .withColumn(
        "_dq_warning_rules",
        F.when(
            F.size(F.col("_dq_warning_rules")) > 0,
            F.concat_ws("; ", F.col("_dq_warning_rules"))
        ).otherwise(
            F.lit(None).cast("string")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_strategy = WriteStrategy.strip().upper()

print(f"Write Strategy : {write_strategy}")

if write_strategy != "OVERWRITE":
    raise ValueError(
        f"Unsupported Silver WriteStrategy '{WriteStrategy}' "
        f"for entity: {EntityName}. "
        f"PULSE Silver uses OVERWRITE because it is a transient layer."
    )


(
    valid_output_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_target_path)
)

print(
    f"Silver table refreshed successfully: "
    f"{TargetSchema}.{TargetTable}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Persist Quarantined Records
# 
# Records failing quarantine-level DQ rules are stored separately from valid Silver data.
# 
# For full-load entities, the quarantine table is refreshed on every run.
# 
# For incremental entities, rejected records are merged using their deterministic record hash to prevent duplicate quarantine records during retries.

# CELL ********************

quarantine_target_path = (
    f"abfss://{silver_workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{silver_lakehouse_id}/"
    f"Tables/{QuarantineSchema}/{QuarantineTable}"
)

print(
    f"Quarantine Target : "
    f"{QuarantineSchema}.{QuarantineTable}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare quarantine records for physical write
# Convert array columns to SQL-compatible strings

quarantine_output_df = (
    quarantine_df
    .withColumn(
        "_dq_failed_rules",
        F.when(
            F.size(F.col("_dq_failed_rules")) > 0,
            F.concat_ws("; ", F.col("_dq_failed_rules"))
        ).otherwise(
            F.lit(None).cast("string")
        )
    )
    .withColumn(
        "_dq_warning_rules",
        F.when(
            F.size(F.col("_dq_warning_rules")) > 0,
            F.concat_ws("; ", F.col("_dq_warning_rules"))
        ).otherwise(
            F.lit(None).cast("string")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Refresh quarantine table for the current Silver processing batch

(
    quarantine_output_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(quarantine_target_path)
)

print(
    f"Quarantine refreshed successfully. "
    f"Rows: {quarantine_row_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Silver Write Reconciliation
# 
# After the Silver write completes, the target Delta table is read back and reconciled against the number of valid records produced by the notebook.
# 
# For the transient Silver layer, the expected target row count must equal the valid row count for the current processing batch.
# 
# A mismatch causes the notebook execution to fail.

# CELL ********************

# Read Silver target back after write

silver_target_df = (
    spark.read
         .format("delta")
         .load(silver_target_path)
)

silver_target_row_count = silver_target_df.count()

print(f"Valid Rows Expected : {valid_row_count:,}")
print(f"Silver Target Rows  : {silver_target_row_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reconcile notebook output with physical Silver table

if silver_target_row_count != valid_row_count:

    raise ValueError(
        f"Silver reconciliation failed for {EntityName}. "
        f"Expected={valid_row_count}, "
        f"Actual={silver_target_row_count}"
    )

print("Silver Reconciliation : PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
