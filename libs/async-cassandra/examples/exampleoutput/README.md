# Example Output Directory

This directory is used by the async-cassandra examples to store output files such as:
- CSV exports
- Parquet exports
- Any other generated files

All files in this directory (except .gitignore and README.md) are ignored by git.

## Configuring Output Location

You can override the output directory using the `EXAMPLE_OUTPUT_DIR` environment variable:

```bash
# From the libs/async-cassandra directory:
cd libs/async-cassandra
EXAMPLE_OUTPUT_DIR=/tmp/my-output make example-export-csv
```

## Cleaning Up

To remove all generated files:
```bash
# From the libs/async-cassandra directory:
cd libs/async-cassandra
rm -rf examples/exampleoutput/*
# Or just remove specific file types
rm -f examples/exampleoutput/*.csv
rm -f examples/exampleoutput/*.parquet
```
