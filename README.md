# elk-collectd
[Collectd](https://collectd.org) plugins to get some stats and metrics from [Elasticsearch](https://www.elastic.co/products/elasticsearch) and [Logstash](https://www.elastic.co/products/logstash). Using [collectd-python](https://collectd.org/documentation/manpages/collectd-python.5.shtml).

## Requirements
 * collectd 4.9+
 * python 2.6+
 * Elasticsearch 6
 * Logstash 6
 
## Installation
1. Place *.py files into a directory on the host. For example into `/usr/share/collectd/plugins`.
2. Configure collectd (see below).
3. Restart collectd.

## Configuration
1. `collectd.conf`:
```
LoadPlugin python

<Plugin python>
  ModulePath "/usr/share/collectd/plugins"
  Import "elasticsearch-collectd"
  <Module "elasticsearch-collectd">
    Host "localhost" # elasticsearch host, default is localhost
    Port 9200 # elasticsearch API port, default is 9200
    Name "elasticsearch" # instance name, default is elasticsearch
    Stats_enabled "nodes cluster_health" # Which stats you should check, default is to check both nodes and cluster_health
    Timeout 20 # stat fetching timeout in secs, default is 20
  </Module>
<Plugin>

<Plugin python>
  ModulePath "/usr/share/collectd/plugins"
  Import "logstash-collectd"
  <Module "logstash-collectd">
    Host "localhost" # logstash host, default is localhost
    Port 9600 # logstash API port, default is 9600
    Name "logstash" # instance name, default is logstash
  </Module>
<Plugin>
```
2. You can use multiple `Module` blocks with different options if you need to collect data from multiple instances.
3. If you skip one or more option or whole `Module` block, default values will be used.
