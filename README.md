# Facebook Presto input plugin for Embulk [![Gem Version](https://badge.fury.io/rb/embulk-input-presto.svg)](http://badge.fury.io/rb/embulk-input-presto) [![Build Status](https://secure.travis-ci.org/toyama0919/embulk-input-presto.png?branch=master)](http://travis-ci.org/toyama0919/embulk-input-presto)

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
- **columns**(**deprecated**): columns (array, required)
  - **name**: name (string, required)
  - **type**: type (string, required)

**Warning** : **columns** deprecated since over v0.2.0. Support auto fetch schema.

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
out:
  type: stdout
```

## Support type
* TIMESTAMP
* LONG
* DOUBLE
* BOOLEAN
* STRING
* JSON

## Build

```
$ rake
```
