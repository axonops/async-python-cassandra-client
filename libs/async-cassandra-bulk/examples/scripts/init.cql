-- Initialize keyspace and tables for bulk operations example
-- This script creates test data for demonstrating token-aware bulk operations

-- Create keyspace with NetworkTopologyStrategy for production-like setup
CREATE KEYSPACE IF NOT EXISTS bulk_ops
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
}
AND durable_writes = true;

-- Use the keyspace
USE bulk_ops;

-- Create a large table for bulk operations testing
CREATE TABLE IF NOT EXISTS large_dataset (
    id UUID,
    partition_key INT,
    clustering_key INT,
    data TEXT,
    value DOUBLE,
    created_at TIMESTAMP,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY (partition_key, clustering_key, id)
) WITH CLUSTERING ORDER BY (clustering_key ASC, id ASC)
  AND compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Create an index for testing
CREATE INDEX IF NOT EXISTS idx_created_at ON large_dataset (created_at);

-- Create a table for export/import testing
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID,
    customer_id UUID,
    order_date DATE,
    order_time TIMESTAMP,
    total_amount DECIMAL,
    status TEXT,
    items LIST<FROZEN<MAP<TEXT, TEXT>>>,
    shipping_address MAP<TEXT, TEXT>,
    PRIMARY KEY ((customer_id), order_date, order_id)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id ASC)
  AND compression = {'class': 'LZ4Compressor'};

-- Create a simple counter table
CREATE TABLE IF NOT EXISTS page_views (
    page_id UUID,
    date DATE,
    views COUNTER,
    PRIMARY KEY ((page_id), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Create a time series table
CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id UUID,
    bucket TIMESTAMP,
    reading_time TIMESTAMP,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    location FROZEN<MAP<TEXT, DOUBLE>>,
    PRIMARY KEY ((sensor_id, bucket), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC)
  AND compression = {'class': 'LZ4Compressor'}
  AND default_time_to_live = 2592000; -- 30 days TTL

-- Grant permissions (if authentication is enabled)
-- GRANT ALL ON KEYSPACE bulk_ops TO cassandra;

-- Display confirmation
SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = 'bulk_ops';
