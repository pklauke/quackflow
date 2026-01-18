import pyarrow as pa
import pytest

from quackflow._internal.repartition import compute_partition, repartition


class TestComputePartition:
    def test_consistent_hashing(self):
        # Same values should always hash to same partition
        partition1 = compute_partition(("alice", 1), 4)
        partition2 = compute_partition(("alice", 1), 4)
        assert partition1 == partition2

    def test_different_values_can_have_different_partitions(self):
        # Different values might hash to different partitions
        partitions = set()
        for i in range(100):
            partition = compute_partition((f"user_{i}",), 4)
            partitions.add(partition)
        # Should have multiple partitions represented
        assert len(partitions) > 1


class TestRepartition:
    def test_empty_batch_returns_empty(self):
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        batch = pa.RecordBatch.from_pydict({"id": [], "name": []}, schema=schema)

        result = repartition(batch, ["id"], 2)
        assert result == {}

    def test_single_key_repartition(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["alice", "bob", "charlie", "david", "eve"],
            }
        )

        result = repartition(batch, ["id"], 2)

        # All rows should be distributed
        total_rows = sum(b.num_rows for b in result.values())
        assert total_rows == 5

        # Each partition should have the correct schema
        for batch in result.values():
            assert batch.schema.names == ["id", "name"]

    def test_multi_key_repartition(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "region": ["us", "us", "eu", "eu"],
                "category": ["a", "b", "a", "b"],
                "value": [1, 2, 3, 4],
            }
        )

        result = repartition(batch, ["region", "category"], 4)

        # All rows should be distributed
        total_rows = sum(b.num_rows for b in result.values())
        assert total_rows == 4

    def test_no_keys_broadcasts_to_all(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "value": [10, 20, 30],
            }
        )

        result = repartition(batch, [], 3)

        # Should broadcast to all partitions
        assert len(result) == 3
        for partition_batch in result.values():
            assert partition_batch.num_rows == 3

    def test_same_key_values_go_to_same_partition(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "user_id": ["alice", "bob", "alice", "bob", "alice"],
                "event": ["click", "view", "purchase", "click", "view"],
            }
        )

        result = repartition(batch, ["user_id"], 4)

        # Collect all alice and bob rows
        alice_partitions = set()
        bob_partitions = set()

        for partition_id, partition_batch in result.items():
            users = partition_batch.column("user_id").to_pylist()
            if "alice" in users:
                alice_partitions.add(partition_id)
            if "bob" in users:
                bob_partitions.add(partition_id)

        # All alice rows should be in same partition
        assert len(alice_partitions) == 1
        # All bob rows should be in same partition
        assert len(bob_partitions) == 1

    def test_preserves_data_integrity(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "value": [100, 200, 300, 400],
            }
        )

        result = repartition(batch, ["id"], 2)

        # Collect all rows from all partitions
        all_ids = []
        all_values = []
        for partition_batch in result.values():
            all_ids.extend(partition_batch.column("id").to_pylist())
            all_values.extend(partition_batch.column("value").to_pylist())

        # All original data should be present
        assert sorted(all_ids) == [1, 2, 3, 4]
        assert sorted(all_values) == [100, 200, 300, 400]
