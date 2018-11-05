#!/usr/bin/env python

import collectd
import collections
import json
import urllib2

CONFIGS = []
CONFIG_DEFAULT = [{
  "host": "localhost",
  "port": "9700",
  "node": "filebeat",
  "url": "http://localhost:9700/debug/vars"
}]

stat = collections.namedtuple("Stat", ("type", "path"))

# Metrics dictionary
STATS = {
  # Harvesters:
  "filebeat.harvester.closed": stat("gauge", "filebeat.harvester.closed"),
  "filebeat.harvester.files_truncated": stat("counter", "filebeat.harvester.files.truncated"),
  "filebeat.harvester.open_files": stat("gauge", "filebeat.harvester.open_files"),
  "filebeat.harvester.running": stat("gauge", "filebeat.harvester.running"),
  "filebeat.harvester.skipped": stat("counter", "filebeat.harvester.skipped"),
  "filebeat.harvester.started": stat("gauge", "filebeat.harvester.started"),

  # Prospectors:
  "filebeat.prospector.log_files_renamed": stat("counter", "filebeat.prospector.log.files.renamed"),
  "filebeat.prospector.log_files_truncated": stat("counter", "filebeat.prospector.log.files.truncated"),

  # Config:
  "libbeat.config.module.running": stat("counter", "libbeat.config.module.running"),
  "libbeat.config.module.starts": stat("counter", "libbeat.config.module.starts"),
  "libbeat.config.module.stops": stat("counter", "libbeat.config.module.stops"),
  "libbeat.config.reloads": stat("counter", "libbeat.config.reloads"),

  # Outputs:
  "libbeat.es.call_count.publish_events": stat("counter", "libbeat.es.call_count.PublishEvents"),
  "libbeat.es.publish.read_bytes": stat("counter", "libbeat.es.publish.read_bytes"),
  "libbeat.es.publish.read_errors": stat("counter", "libbeat.es.publish.read_errors"),
  "libbeat.es.publish.write_bytes": stat("counter", "libbeat.es.publish.write_bytes"),
  "libbeat.es.publish.write_errors": stat("counter", "libbeat.es.publish.write_errors"),
  "libbeat.es.published_and_acked_events": stat("counter", "libbeat.es.published_and_acked_events"),
  "libbeat.es.published_but_not_acked_events": stat("counter", "libbeat.es.published_but_not_acked_events"),
  "libbeat.kafka.call_count.publish_events": stat("counter", "libbeat.kafka.call_count.PublishEvents"),
  "libbeat.kafka.published_and_acked_events": stat("counter", "libbeat.kafka.published_and_acked_events"),
  "libbeat.kafka.published_but_not_acked_events": stat("counter", "libbeat.kafka.published_but_not_acked_events"),
  "libbeat.logstash.call_count.publish_events": stat("counter", "libbeat.logstash.call_count.PublishEvents"),
  "libbeat.logstash.publish.read_bytes": stat("counter", "libbeat.logstash.publish.read_bytes"),
  "libbeat.logstash.publish.read_errors": stat("counter", "libbeat.logstash.publish.read_errors"),
  "libbeat.logstash.publish.write_bytes": stat("counter", "libbeat.logstash.publish.write_bytes"),
  "libbeat.logstash.publish.write_errors": stat("counter", "libbeat.logstash.publish.write_errors"),
  "libbeat.logstash.published_and_acked_events": stat("counter", "libbeat.logstash.published_and_acked_events"),
  "libbeat.logstash.published_but_not_acked_events": stat("counter", "libbeat.logstash.published_but_not_acked_events"),
  "libbeat.outputs.messages_dropped": stat("counter", "libbeat.outputs.messages_dropped"),
  "libbeat.publisher.messages_in_worker_queues": stat("counter", "libbeat.publisher.messages_in_worker_queues"),
  "libbeat.publisher.published_events": stat("counter", "libbeat.publisher.published_events"),
  "libbeat.redis.publish.read_bytes": stat("counter", "libbeat.redis.publish.read_bytes"),
  "libbeat.redis.publish.read_errors": stat("counter", "libbeat.redis.publish.read_errors"),
  "libbeat.redis.publish.write_bytes": stat("counter", "libbeat.redis.publish.write_bytes"),
  "libbeat.redis.publish.write_errors": stat("counter", "libbeat.redis.publish.write_errors"),
  "publish.events": stat("counter", "publish.events"),

  # Memory:
  "memstats.alloc": stat("counter", "memstats/Alloc"),
  "memstats.buck_hash_sys": stat("counter", "memstats/BuckHashSys"),
  "memstats.frees": stat("counter", "memstats/Frees"),
  "memstats.gc_cpu_fraction": stat("counter", "memstats/GCCPUFraction"),
  "memstats.gc_sys": stat("counter", "memstats/GCSys"),
  "memstats.heap_alloc": stat("counter", "memstats/HeapAlloc"),
  "memstats.heap_idle": stat("counter", "memstats/HeapIdle"),
  "memstats.heap_inuse": stat("counter", "memstats/HeapInuse"),
  "memstats.heap_objects": stat("counter", "memstats/HeapObjects"),
  "memstats.heap_released": stat("counter", "memstats/HeapReleased"),
  "memstats.heap_sys": stat("counter", "memstats/HeapSys"),
  "memstats.last_gc": stat("counter", "memstats/LastGC"),
  "memstats.lookups": stat("counter", "memstats/Lookups"),
  "memstats.m_cache_inuse": stat("counter", "memstats/MCacheInuse"),
  "memstats.m_cache_sys": stat("counter", "memstats/MCacheSys"),
  "memstats.m_span_inuse": stat("counter", "memstats/MSpanInuse"),
  "memstats.m_span_sys": stat("counter", "memstats/MSpanSys"),
  "memstats.mallocs": stat("counter", "memstats/Mallocs"),
  "memstats.next_gc": stat("counter", "memstats/NextGC"),
  "memstats.num_gc": stat("counter", "memstats/NumGC"),
  "memstats.other_sys": stat("counter", "memstats/OtherSys"),
  "memstats.pause_total_ns": stat("counter", "memstats/PauseTotalNs"),
  "memstats.stack_inuse": stat("counter", "memstats/StackInuse"),
  "memstats.stack_sys": stat("counter", "memstats/StackSys"),
  "memstats.sys": stat("counter", "memstats/Sys"),
  "memstats.total_alloc": stat("counter", "memstats/TotalAlloc"),

  # Registry
  "registrar.states.cleanup": stat("counter", "registrar.states.cleanup"),
  "registrar.states.current": stat("counter", "registrar.states.current"),
  "registrar.states.update": stat("counter", "registrar.states.update"),
  "registrar.writes": stat("counter", "registrar.writes"),

}

def extract_value(json, path):
  val = json
  for el in path.split("/"):
    val = val[el]
  return val

def fetch_stats():
  global CONFIGS
  if not CONFIGS: CONFIGS = CONFIG_DEFAULT
  for config in CONFIGS:
    try:
      stats = json.load(urllib2.urlopen(config["url"], timeout=10))
    except Exception as err:
      collectd.error("Filebeat plugin ("+config["node"]+"): Error fetching stats from "+config["url"]+": "+str(err))
      return None
    parse_stats(stats, config)

def parse_stats(json, config):
  for name, stat in STATS.iteritems():
    try:
      value = extract_value(json, STATS[name].path)
    except Exception as err:
      collectd.warning("Filebeat plugin ("+config["node"]+"): Could not process path "+STATS[name].path+": "+str(err))
      continue
    dispatch_stat(name, value, stat.type, config)

def dispatch_stat(stat_name, stat_value, stat_type, config):
  val = collectd.Values(plugin=config["node"])
  val.type_instance = stat_name
  val.values = [stat_value]
  val.type = stat_type
  val.dispatch()

def read_callback():
  stats = fetch_stats()

def config_callback(config):
  global CONFIGS
  for config in config.children:
    host = CONFIG_DEFAULT[0]["host"]
    port = CONFIG_DEFAULT[0]["port"]
    node = CONFIG_DEFAULT[0]["node"]
    if config.key == "Host": host = str(config.values[0])
    elif config.key == "Port": port = str(int(config.values[0]))
    elif config.key == "Name": node = str(config.values[0])
    else: collectd.warning("Filebeat plugin: Unknown config key "+config.key)
    CONFIGS.append({
      "host": host,
      "port": port,
      "node": node,
      "url": "http://"+host+":"+port+"/debug/vars"
    })

collectd.register_config(config_callback)
collectd.register_read(read_callback)
