# API Reference

## BulkOperator

Main interface for bulk operations on Cassandra tables.

### Constructor

```python
BulkOperator(session: AsyncCassandraSession)
```

**Parameters:**
- `session`: Active async Cassandra session

### Methods

#### count()

Count rows in a table with optional filtering.

```python
async def count(
    table: str,
    where: Optional[str] = None
) -> int
```

**Parameters:**
- `table`: Table name (can include keyspace as `keyspace.table`)
- `where`: Optional WHERE clause (without the WHERE keyword)

**Returns:** Number of rows

**Example:**
```python
# Count all rows
total = await operator.count('users')

# Count with filter
active = await operator.count('users', 'active = true ALLOW FILTERING')
```

#### export()

Export table data to a file.

```python
async def export(
    table: str,
    output_path: str,
    format: str = 'csv',
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    concurrency: int = 4,
    batch_size: int = 1000,
    page_size: int = 5000,
    progress_callback: Optional[Callable] = None,
    checkpoint_interval: Optional[int] = None,
    checkpoint_callback: Optional[Callable] = None,
    resume_from: Optional[Dict] = None,
    csv_options: Optional[Dict] = None,
    json_options: Optional[Dict] = None
) -> BulkOperationStats
```

**Parameters:**
- `table`: Table to export
- `output_path`: Output file path
- `format`: Export format ('csv' or 'json')
- `columns`: Specific columns to export (default: all)
- `where`: Not supported for export (use views or post-processing)
- `concurrency`: Number of parallel workers
- `batch_size`: Rows per batch
- `page_size`: Cassandra query page size
- `progress_callback`: Function called with BulkOperationStats
- `checkpoint_interval`: Seconds between checkpoints
- `checkpoint_callback`: Function called with checkpoint state
- `resume_from`: Previous checkpoint to resume from
- `csv_options`: CSV format options
- `json_options`: JSON format options

**Returns:** BulkOperationStats with export results

## BulkOperationStats

Statistics and progress information for bulk operations.

### Attributes

- `rows_processed`: Total rows processed
- `duration_seconds`: Operation duration
- `rows_per_second`: Processing rate
- `progress_percentage`: Completion percentage (0-100)
- `ranges_completed`: Number of token ranges completed
- `ranges_total`: Total number of token ranges
- `is_complete`: Whether operation completed successfully
- `errors`: List of errors encountered

### Methods

```python
def to_dict() -> Dict[str, Any]
```

Convert statistics to dictionary format.

## Exporters

### CSVExporter

Export data to CSV format.

```python
CSVExporter(
    output_path: str,
    options: Optional[Dict] = None
)
```

**Options:**
- `delimiter`: Field delimiter (default: ',')
- `null_value`: String for NULL values (default: '')
- `escape_char`: Escape character (default: '\\')
- `quote_char`: Quote character (default: '"')

### JSONExporter

Export data to JSON format.

```python
JSONExporter(
    output_path: str,
    options: Optional[Dict] = None
)
```

**Options:**
- `mode`: 'array' (JSON array) or 'objects' (JSONL)
- `pretty`: Pretty-print with indentation (default: False)

### BaseExporter

Abstract base class for custom exporters.

```python
class BaseExporter(ABC):
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize exporter resources."""

    @abstractmethod
    async def write_header(self, columns: List[str]) -> None:
        """Write header/schema information."""

    @abstractmethod
    async def write_row(self, row: Dict) -> None:
        """Write a single row."""

    @abstractmethod
    async def finalize(self) -> None:
        """Cleanup and close resources."""
```

## ParallelExporter

Low-level parallel export implementation.

```python
ParallelExporter(
    session: AsyncCassandraSession,
    table: str,
    exporter: BaseExporter,
    columns: Optional[List[str]] = None,
    concurrency: int = 4,
    batch_size: int = 1000,
    page_size: int = 5000,
    progress_callback: Optional[Callable] = None,
    checkpoint_callback: Optional[Callable] = None,
    checkpoint_interval: Optional[int] = None,
    resume_from: Optional[Dict] = None
)
```

### Methods

```python
async def export() -> BulkOperationStats
```

Execute the parallel export operation.

## Utility Functions

### Token Utilities

```python
from async_cassandra_bulk.utils.token_utils import (
    discover_token_ranges,
    split_token_range
)

# Discover token ranges for a keyspace
ranges = await discover_token_ranges(session, 'my_keyspace')

# Split a range for better parallelism
sub_ranges = split_token_range(token_range, num_splits=4)
```

### Type Conversions

The library automatically handles Cassandra type conversions:

```python
# Automatic conversions in exporters:
# UUID -> string
# Timestamp -> ISO 8601 string
# Collections -> JSON representation
# Boolean -> 'true'/'false' (CSV) or true/false (JSON)
# Decimal -> string representation
```

## Error Handling

```python
from async_cassandra_bulk import BulkOperationError

try:
    stats = await operator.export(table='my_table', output_path='out.csv')
except BulkOperationError as e:
    print(f"Export failed: {e}")
    if hasattr(e, 'stats'):
        print(f"Partial progress: {e.stats.rows_processed} rows")
```

## Best Practices

1. **Concurrency**: Start with default (4) and increase based on cluster size
2. **Batch Size**: 1000-5000 rows typically optimal
3. **Checkpointing**: Enable for exports taking >5 minutes
4. **Memory**: For very large exports, use JSONL format with smaller batches
5. **Progress Tracking**: Implement callbacks for user feedback on long operations
