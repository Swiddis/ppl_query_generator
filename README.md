# PPL Query Generator

A script to generate meaningful(ish) PPL queries given sufficiently vibrant sample data.

## Setup

The project has no external dependencies at the moment: any Python 3.10+ interpreter will do.

## Usage

As this is still a prototype, there's no formal CLI interface.

1. Add json data to `data`.
2. Convert the data to a schema with `make_schema.py`. Ideally, the name of the output file should
   match the target index for the queries, as it's what's used as the `source` location.
   `python3 make_schema.py data/nginx_raw.json schemas/ss4o_logs-nginx-sample-sample.json`
3. Run `gen_queries.py` with the schema name and an optional quantity (default 10).
   `python3 gen_queries.py ss4o_logs-nginx-sample-sample 30`.

### Example

```sh
> python3 gen_queries.py ss4o_logs-nginx-sample-sample 20
source = ss4o_logs-nginx-sample-sample | sort - communication.source.ip | rename trace_id as id | fields event.name, @timestamp, http.flavor, http.url | where http.flavor != '1.1' | rare @timestamp by event.name
source = ss4o_logs-nginx-sample-sample | fields http.flavor, http.url, communication.source.ip | sort communication.source.ip | rename http.url as url | dedup http.flavor
source = ss4o_logs-nginx-sample-sample | rename http.response.status_code as code | sort http.response.bytes
source = ss4o_logs-nginx-sample-sample | fields attributes.data_stream.dataset, @timestamp, event.name | rename event.name as name | where name = 'access' OR attributes.data_stream.dataset = 'nginx.access' XOR @timestamp = TIMESTAMP('2023-06-19 09:59:13') | sort - @timestamp | rare @timestamp
source = ss4o_logs-nginx-sample-sample | stats max(http.response.bytes), min(http.response.bytes), avg(http.response.bytes) by event.name
source = ss4o_logs-nginx-sample-sample | sort - communication.source.ip | rename attributes.data_stream.type as type | where event.domain = 'nginx.access' XOR @timestamp < TIMESTAMP('2023-06-19 09:59:12') | fields span_id | top 20 span_id
source = ss4o_logs-nginx-sample-sample | fields attributes.data_stream.namespace, event.type, event.result, event.name, event.category | rename attributes.data_stream.namespace as namespace | where event.type = 'access' OR event.result = 'success' | stats count() by event.result
source = ss4o_logs-nginx-sample-sample | sort body | where trace_id != '102981ABCD2901' OR http.response.bytes = 2895 XOR communication.source.ip LIKE '%69' | rename event.type as type | dedup attributes.data_stream.type
source = ss4o_logs-nginx-sample-sample | rename attributes.data_stream.dataset as dataset | sort http.response.bytes
source = ss4o_logs-nginx-sample-sample | rename http.flavor as flavor | dedup span_id
source = ss4o_logs-nginx-sample-sample | where http.response.bytes <= 1477 XOR @timestamp >= TIMESTAMP('2023-06-19 09:59:11') | fields span_id | rename span_id as id
source = ss4o_logs-nginx-sample-sample | dedup event.name
source = ss4o_logs-nginx-sample-sample | rename communication.source.address as address | sort @timestamp | rare trace_id by http.response.bytes
source = ss4o_logs-nginx-sample-sample | where http.response.status_code = '400' XOR span_id = 'abcdef1010' | top 5 observedTimestamp by attributes.data_stream.namespace
source = ss4o_logs-nginx-sample-sample | where event.category != 'web' AND attributes.data_stream.dataset = 'nginx.access' | fields communication.source.ip, observedTimestamp, event.category, http.response.status_code | head 20
source = ss4o_logs-nginx-sample-sample | fields http.request.method, span_id | top 20 span_id by http.request.method
source = ss4o_logs-nginx-sample-sample | stats min(http.response.bytes), avg(http.response.bytes)
source = ss4o_logs-nginx-sample-sample | stats max(http.response.bytes)
source = ss4o_logs-nginx-sample-sample | fields event.category, event.result, communication.source.ip, attributes.data_stream.type, event.name | rename event.name as name | where communication.source.ip < '111.51.133.169' | head
source = ss4o_logs-nginx-sample-sample | rename span_id as id | where NOT body > '202.179.32.148 - - [19/Jun/2023:16:59:05 +0000] "DELETE /array%20Horizontal.css HTTP/1.1" 200 949 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_4 rv:5.0; en-US) AppleWebKit/532.32.4 (KHTML, like Gecko) Version/5.1 Safari/532.32.4"' XOR attributes.data_stream.namespace = 'production' | fields event.type, event.result, event.category | dedup event.category
```
