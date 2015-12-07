# Facebook Presto input plugin for Embulk

Facebook Presto input plugin for Embulk.
[see](https://prestodb.io/).

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration

- **host**: host (string, default: `"localhost"`)
- **port**: port (integer, default: `8080`)
- **schema**: schema (string, default: `"default"`)
- **catalog**: catalog (string, default: `"native"`)
- **query**: query (string, required)
- **user**: user (string, default: `"embulk"`)
- **columns**: columns (array, required)

## Example

```yaml
in:
  type: presto
  host: presto-cordinator
  catalog: store
  schema: public
  query: |
    SELECT
      trim(upper(url_decode(keyword))) AS keyword,
      count(*) as count
    FROM search
    CROSS JOIN UNNEST(split(keywords, ',')) AS t (keyword)
    WHERE log_date >= (CURRENT_DATE - INTERVAL '90' DAY)
     AND length(keywords) != 256
    group by keyword
    having count(*) >= 10
    order by count(*) desc
  columns:
    - {name: keyword, type: string}
    - {name: count, type: long}
out:
  type: stdout
```

## Limited
* Only the data type that Embulk supports is possible.
  * TIMESTAMP
  * LONG
  * DOUBLE
  * BOOLEAN
  * STRING

* Presto is not support Prepared statement.
  * Can't fetch schema by sql

## Build

```
$ rake
```
