"""Repartitioning logic for distributing data across partitions."""

import hashlib
from typing import Any

import pyarrow as pa


def compute_partition(values: tuple[Any, ...], num_partitions: int) -> int:
    """Compute partition ID for a tuple of values using consistent hashing."""
    # Create a string representation of the values
    key_str = "|".join(str(v) for v in values)
    # Hash and mod to get partition
    hash_value = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
    return hash_value % num_partitions


def repartition(
    batch: pa.RecordBatch,
    keys: list[str],
    num_partitions: int,
) -> dict[int, pa.RecordBatch]:
    """
    Hash partition a batch by the given keys.

    Args:
        batch: Input record batch
        keys: Column names to partition by
        num_partitions: Number of output partitions

    Returns:
        Dictionary mapping partition_id -> record batch for that partition
    """
    if batch.num_rows == 0:
        return {}

    if not keys:
        # No partition key - broadcast to all partitions
        return {i: batch for i in range(num_partitions)}

    # Get the key columns
    key_columns = [batch.column(k) for k in keys]

    # Compute partition for each row
    row_partitions: list[int] = []
    for row_idx in range(batch.num_rows):
        values = tuple(col[row_idx].as_py() for col in key_columns)
        partition = compute_partition(values, num_partitions)
        row_partitions.append(partition)

    # Group rows by partition
    partition_indices: dict[int, list[int]] = {i: [] for i in range(num_partitions)}
    for row_idx, partition in enumerate(row_partitions):
        partition_indices[partition].append(row_idx)

    # Create output batches
    result: dict[int, pa.RecordBatch] = {}
    for partition_id, indices in partition_indices.items():
        if indices:
            # Use take to select rows for this partition
            indices_array = pa.array(indices)
            partition_batch = batch.take(indices_array)
            result[partition_id] = partition_batch

    return result
