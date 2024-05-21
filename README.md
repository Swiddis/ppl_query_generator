# PPL Query Generator

A script to generate meaningful(ish) PPL queries given sufficiently vibrant sample data.

## Setup

The project has no external dependencies at the moment: any Python 3.10+ interpreter will do.

## Usage

As this is still a prototype, there's no formal CLI interface.

1. Add json data to `data`.
2. In `make_schema.py` update `DATA_FILE` and `SCHEMA_FILE`. Run it. You should see a
   corresponding file in `schemas`. For best results, the schema file name should match
   the OS Index you plan on querying, as it's what's used as the `source` in the
   generated queries.
3. In `gen_queries.py` update `SCHEMA_NAME` to the new value. Run it. Optionally pass a
   number after the script to change the amount: `python3 gen_queries.py 50`.
