# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Unified Cassandra control interface for tests (`CassandraControl`)
- CI environment detection for test skipping
- Support for both Docker and Podman in all test fixtures
- Consistent container naming across all test suites
- Production-like Cassandra configuration with GossipingPropertyFileSnitch

### Changed
- Updated all Cassandra container configurations to use 4GB memory and 3GB heap
- Refactored integration test fixtures to use shared clusters
- Updated documentation to reflect protocol v5 as highest supported version
- Standardized on Cassandra 5 across all examples and tests
- Fixed mock future callbacks to use `asyncio.get_running_loop().call_soon()`

### Fixed
- Integration tests hanging due to improper async mock callbacks
- "Cluster is already shut down" errors in integration tests
- IPv6 connection issues by forcing IPv4 (127.0.0.1)
- BDD FastAPI reconnection tests not properly isolating test data
- "Cannot schedule new futures after shutdown" errors
- CI test failures when trying to control Cassandra service containers

### Security
- No security changes in this release

## [0.0.1] - TBD

Initial release - library is under active development.
