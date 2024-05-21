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
3. Run `gen_queries.py` with the schema name and an optional quantity.
   `python3 gen_queries.py ss4o_logs-nginx-sample-sample 30`.

### Example

```sh
> python3 gen_queries.py ss4o_logs-nginx-sample-sample 20
source = ss4o_logs-nginx-sample-sample | fields event.domain, event.category, http.flavor, body | where event.domain = 'nginx.access' | top 1 event.category by body
source = ss4o_logs-nginx-sample-sample | stats sum(http.response.bytes)
source = ss4o_logs-nginx-sample-sample | sort @timestamp | fields http.flavor, http.request.method, span_id | where http.flavor = '1.1' | fields span_id | top 1 span_id
source = ss4o_logs-nginx-sample-sample | fields event.category | where event.category != 'web' | where event.category != 'web' | fields event.category
source = ss4o_logs-nginx-sample-sample | fields trace_id, @timestamp
source = ss4o_logs-nginx-sample-sample | fields observedTimestamp, http.url, http.response.bytes, event.domain, http.request.method | where event.domain != 'nginx.access' | dedup http.url
source = ss4o_logs-nginx-sample-sample | rename http.request.method as method | fields method, observedTimestamp, attributes.data_stream.type, span_id, attributes.data_stream.namespace | rename span_id as id
source = ss4o_logs-nginx-sample-sample | fields trace_id | rename trace_id as id | fields id
source = ss4o_logs-nginx-sample-sample | rename event.domain as domain | sort communication.source.ip | dedup http.response.bytes
source = ss4o_logs-nginx-sample-sample | rename attributes.data_stream.dataset as dataset | sort communication.source.ip | dedup event.result
source = ss4o_logs-nginx-sample-sample | where http.response.status_code != '400' | sort - @timestamp | fields http.response.bytes, communication.source.address, http.request.method, event.category | where http.request.method != 'DELETE'
source = ss4o_logs-nginx-sample-sample | where http.request.method != 'DELETE'
source = ss4o_logs-nginx-sample-sample | sort @timestamp | where communication.source.address = '127.0.0.1'
source = ss4o_logs-nginx-sample-sample | rename event.category as category | rare http.url by span_id
source = ss4o_logs-nginx-sample-sample | where event.type = 'access' | where communication.source.ip < '237.103.69.52' | dedup http.request.method
source = ss4o_logs-nginx-sample-sample | fields http.request.method, body, @timestamp, event.kind | sort body
source = ss4o_logs-nginx-sample-sample | rare http.response.status_code
source = ss4o_logs-nginx-sample-sample | stats sum(http.response.bytes), avg(http.response.bytes)
source = ss4o_logs-nginx-sample-sample | fields communication.source.address, http.request.method, event.result | fields communication.source.address, http.request.method, event.result | rare communication.source.address by event.result
source = ss4o_logs-nginx-sample-sample | fields body, http.response.bytes, attributes.data_stream.type, http.flavor, communication.source.address | fields http.flavor, communication.source.address, attributes.data_stream.type, http.response.bytes, body | fields http.response.bytes, http.flavor, body, communication.source.address, attributes.data_stream.type | where communication.source.address = '127.0.0.1'
```
