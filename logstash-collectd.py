#!/usr/bin/env python

import collectd
import collections
import json
import urllib2

CONFIGS = []
CONFIG_DEFAULT = [{
  "host": "localhost",
  "port": "9600",
  "node": "logstash",
  "url": "http://localhost:9600/_node/stats"
}]

stat = collections.namedtuple('Stat', ('type', 'path'))

# Metrics dictionary
STATS = {
  # Threads
  'jvm.threads.count': stat('gauge', 'jvm.threads.count'),
  'jvm.threads.peak_count': stat('gauge', 'jvm.threads.peak_count'),

  # Memory:
  'jvm.mem.heap_max': stat('gauge', 'jvm.mem.heap_max_in_bytes'),
  'jvm.mem.heap_used': stat('gauge', 'jvm.mem.heap_used_in_bytes'),
  'jvm.mem.heap_used_percent': stat('gauge', 'jvm.mem.heap_used_percent'),
  'jvm.mem.heap_committed': stat('gauge', 'jvm.mem.heap_committed_in_bytes'),
  'jvm.mem.non_heap_used': stat('gauge', 'jvm.mem.non_heap_used_in_bytes'),
  'jvm.mem.non_heap_committed': stat('gauge', 'jvm.mem.non_heap_committed_in_bytes'),

  # Pools:
  'jvm.mem.survivor_used': stat('gauge', 'jvm.mem.pools.survivor.used_in_bytes'),
  'jvm.mem.survivor_peak_used': stat('gauge', 'jvm.mem.pools.survivor.peak_used_in_bytes'),
  'jvm.mem.survivor_max': stat('gauge', 'jvm.mem.pools.survivor.max_in_bytes'),
  'jvm.mem.survivor_peak_max': stat('gauge', 'jvm.mem.pools.survivor.peak_max_in_bytes'),
  'jvm.mem.survivor_committed': stat('gauge', 'jvm.mem.pools.survivor.committed_in_bytes'),

  'jvm.mem.old_used': stat('gauge', 'jvm.mem.pools.old.used_in_bytes'),
  'jvm.mem.old_peak_used': stat('gauge', 'jvm.mem.pools.old.peak_used_in_bytes'),
  'jvm.mem.old_max': stat('gauge', 'jvm.mem.pools.old.max_in_bytes'),
  'jvm.mem.old_peak_max': stat('gauge', 'jvm.mem.pools.old.peak_max_in_bytes'),
  'jvm.mem.old_committed': stat('gauge', 'jvm.mem.pools.old.committed_in_bytes'),

  'jvm.mem.young_used': stat('gauge', 'jvm.mem.pools.young.used_in_bytes'),
  'jvm.mem.young_peak_used': stat('gauge', 'jvm.mem.pools.young.peak_used_in_bytes'),
  'jvm.mem.young_max': stat('gauge', 'jvm.mem.pools.young.max_in_bytes'),
  'jvm.mem.young_peak_max': stat('gauge', 'jvm.mem.pools.young.peak_max_in_bytes'),
  'jvm.mem.young_committed': stat('gauge', 'jvm.mem.pools.young.committed_in_bytes'),

  # GC:
  'jvm.gc.old_time': stat('counter', 'jvm.gc.collectors.old.collection_time_in_millis'),
  'jvm.gc.old_count': stat('counter', 'jvm.gc.collectors.old.collection_count'),
  'jvm.gc.young_time': stat('counter', 'jvm.gc.collectors.young.collection_time_in_millis'),
  'jvm.gc.young_count': stat('counter', 'jvm.gc.collectors.young.collection_count'),

  # Process:
  'process.open_files': stat('gauge', 'process.open_file_descriptors'),
  'process.peak_open_files': stat('gauge', 'process.peak_open_file_descriptors'),
  'process.max_open_files': stat('gauge', 'process.max_file_descriptors'),
  'process.total_cpu_time': stat('gauge', 'process.cpu.total_in_millis'),
  'process.total_virtual_memory': stat('gauge', 'process.mem.total_virtual_in_bytes'),

  # Events:
  'events.in': stat('counter', 'events.in'),
  'events.filtered': stat('counter', 'events.filtered'),
  'events.out': stat('counter', 'events.out'),
  'events.duration': stat('counter', 'events.duration_in_millis'),
  'events.queue_push_duration': stat('counter', 'events.queue_push_duration_in_millis'),
}

def fetch_stats():
  global CONFIGS
  if not CONFIGS: CONFIGS = CONFIG_DEFAULT
  for config in CONFIGS:
    try:
      stats = json.load(urllib2.urlopen(config["url"], timeout=10))
    except Exception as err:
      collectd.error("Logstash plugin ("+config["node"]+"): Error fetching stats from "+config["url"]+": "+str(err))
      return None
    parse_stats(stats, config)

def parse_stats(json, config):
  for name, stat in STATS.iteritems():
    path = STATS[name].path.split('.')
    try:
      value = reduce(lambda x, y: x[y], path, json)
    except Exception as err:
      collectd.warning("Logstash plugin ("+config["node"]+"): Could not process path "+STATS[name].path+": "+str(err))
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
    else: collectd.warning("Logstash plugin: Unknown config key "+config.key)
    CONFIGS.append({
      "host": host,
      "port": port,
      "node": node,
      "url": "http://"+host+":"+port+"/_node/stats"
    })

collectd.register_config(config_callback)
collectd.register_read(read_callback)
