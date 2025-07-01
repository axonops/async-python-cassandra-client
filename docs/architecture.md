# Architecture Overview

This document provides a high-level overview of how async-cassandra bridges the gap between the synchronous DataStax Cassandra driver and Python's async/await ecosystem.

## Table of Contents

- [Problem Statement](#problem-statement)
- [Solution Architecture](#solution-architecture)
- [Key Components](#key-components)
- [Execution Flow](#execution-flow)
  - [Query Execution](#query-execution)
  - [Streaming Execution](#streaming-execution)
- [Design Principles](#design-principles)

## Problem Statement

The DataStax Cassandra Python driver uses a thread pool for I/O operations, which can create bottlenecks in async applications:

```mermaid
sequenceDiagram
    participant App as Async Application
    participant Driver as Cassandra Driver
    participant ThreadPool as Thread Pool
    participant Cassandra as Cassandra DB

    App->>Driver: execute(query)
    Driver->>ThreadPool: Submit to thread
    Note over ThreadPool: Thread blocked
    ThreadPool->>Cassandra: Send query
    Cassandra-->>ThreadPool: Response
    ThreadPool-->>Driver: Result
    Driver-->>App: Return result
    Note over App,ThreadPool: Thread pool can become bottleneck<br/>under high concurrency
```

## Solution Architecture

async-cassandra wraps the driver's async operations to provide true async/await support:

```mermaid
sequenceDiagram
    participant App as Async Application
    participant AsyncWrapper as async-cassandra
    participant Driver as Cassandra Driver
    participant EventLoop as Event Loop
    participant Cassandra as Cassandra DB

    App->>AsyncWrapper: await execute(query)
    AsyncWrapper->>Driver: execute_async(query)
    Note over AsyncWrapper: Create Future
    Driver->>Cassandra: Send query (non-blocking)
    AsyncWrapper-->>EventLoop: Register callback
    EventLoop-->>App: Control returned
    Note over App: Can handle other requests
    Cassandra-->>Driver: Response
    Driver-->>AsyncWrapper: Callback triggered
    AsyncWrapper-->>EventLoop: Set Future result
    EventLoop-->>App: Resume coroutine
```

## Key Components

### AsyncCluster
- Wraps the DataStax `Cluster` class
- Manages cluster lifecycle (connect, shutdown)
- Provides async context manager support
- Handles authentication and configuration

### AsyncCassandraSession
- Wraps the DataStax `Session` class
- Converts synchronous operations to async/await
- Provides streaming support for large result sets
- Integrates with metrics collection

### AsyncResultSet
- Wraps query results for async consumption
- Handles paging transparently
- Provides familiar result access methods (one(), all())

### AsyncStreamingResultSet
- Enables memory-efficient processing of large results
- Supports async iteration over rows
- Provides page-level access for batch processing
- Includes progress tracking capabilities

## Execution Flow

### Query Execution

The following diagram shows how a standard query flows through the async wrapper:

```mermaid
sequenceDiagram
    participant User as User Code
    participant Session as AsyncCassandraSession
    participant Handler as AsyncResultHandler
    participant Driver as Cassandra Driver
    participant DB as Cassandra

    User->>Session: await execute(query)
    Session->>Driver: execute_async(query)
    Driver-->>Session: ResponseFuture
    Session->>Handler: new AsyncResultHandler(ResponseFuture)
    Handler->>Handler: Register callbacks
    Session-->>User: Return Future

    Note over User,DB: Async execution in progress

    DB-->>Driver: Query result
    Driver-->>Handler: Trigger callback
    Handler->>Handler: Process result/pages
    Handler-->>User: Resolve Future with AsyncResultSet
```

### Streaming Execution

For large result sets, streaming provides memory-efficient processing:

```mermaid
sequenceDiagram
    participant App as Application
    participant Session as AsyncCassandraSession
    participant Stream as AsyncStreamingResultSet
    participant Handler as StreamingResultHandler
    participant Driver as Cassandra Driver
    participant DB as Cassandra

    App->>Session: await execute_stream(query, config)
    Session->>Driver: execute_async(query)
    Driver-->>Session: ResponseFuture
    Session->>Handler: StreamingResultHandler(future, config)
    Handler->>Stream: Create AsyncStreamingResultSet
    Session-->>App: Return Stream

    loop For each page request
        App->>Stream: async for row in stream
        Stream->>Handler: Request next page
        Handler->>Driver: Fetch page asynchronously
        Driver->>DB: Get page (fetch_size rows)
        DB-->>Driver: Page data
        Driver-->>Handler: Page received
        Handler-->>Stream: Yield rows
        Stream-->>App: Return row
    end
```

## Design Principles

### 1. Thin Wrapper Approach
- We wrap, not reimplement, the DataStax driver
- All driver features remain accessible
- Minimal performance overhead

### 2. True Async/Await Support
- All blocking operations converted to async
- Proper integration with Python's event loop
- No blocking of the event loop

### 3. Memory Efficiency
- Streaming support for large result sets
- Configurable fetch sizes
- Page-based processing options

### 4. Developer Experience
- Familiar async/await syntax
- Context manager support
- Type hints throughout

### 5. Production Ready
- Comprehensive error handling
- Metrics and monitoring built-in
- Battle-tested retry policies

## Important Limitations

While async-cassandra provides async/await syntax, it's important to understand:

1. **The underlying I/O is still synchronous** - The DataStax driver uses blocking sockets in threads
2. **Thread pool constraints apply** - Concurrency is limited by the driver's thread pool size
3. **Not a true async driver** - This is a compatibility layer, not a ground-up async implementation

For more details on these limitations and when to use this wrapper, see [Why an Async Wrapper is Necessary](why-async-wrapper.md).
