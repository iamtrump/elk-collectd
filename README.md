# elk-collectd
[Collectd](https://collectd.org) plugins to get some stats and metrics from [Filebeat](https://www.elastic.co/products/beats/filebeat), [Logstash](https://www.elastic.co/products/logstash) and [Elasticsearch](https://www.elastic.co/products/elasticsearch). Using [collectd-python](https://collectd.org/documentation/manpages/collectd-python.5.shtml).

## Requirements
 * collectd 4.9+
 * python 2.6
 * Filebeat 5.6
 * Logstash 6.2
 * Elasticsearch 6.2

To use it with different version of Filebeat, Logstash or Elasticsearch you probably will need to edit Metrics Dictionary.
 
## Installation
1. Filebeat must be started with enabled http server (`--httpprof [HOST]:PORT`). See [here](https://www.elastic.co/guide/en/beats/filebeat/current/command-line-options.html) for more details.
2. Place *.py files into a directory on the host. For example into `/usr/share/collectd/plugins`.
3. Configure collectd (see below).
4. Restart collectd.

## Configuration
1. `collectd.conf`:
```
LoadPlugin python

<Plugin python>
  ModulePath "/usr/share/collectd/plugins"
  Import "filebeat-collectd"
  <Module "filebeat-collectd">
    Host "localhost" # filebeat host, default is localhost
    Port 9700 # filebeat http server port, default is 9700
    Name "filebeat" # instance name, default is filebeat
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

```
2. You can use multiple `Module` blocks with different options if you need to collect data from multiple instances.
3. If you skip one or more option or whole `Module` block, default values will be used.
