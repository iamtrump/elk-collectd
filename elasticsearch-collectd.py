#! /usr/bin/python

import collectd
import collections
import json
import urllib2

CONFIGS = []
CONFIG_DEFAULT = [{
  "host": "localhost",
  "port": "9200",
  "node": "elasticsearch",
  "url": "http://localhost:9200/_nodes/stats",
  "timeout": 20
}]

stat = collections.namedtuple('Stat', ('type', 'path'))

# Metrics dictionary
STATS = {
  # Threads
  '%s.jvm.threads.count': stat('gauge', 'nodes.%s.jvm.threads.count'),
  '%s.jvm.threads.peak_count': stat('gauge', 'nodes.%s.jvm.threads.peak_count'),

  # Memory:
  '%s.jvm.mem.heap_max': stat('gauge', 'nodes.%s.jvm.mem.heap_max_in_bytes'),
  '%s.jvm.mem.heap_used': stat('gauge', 'nodes.%s.jvm.mem.heap_used_in_bytes'),
  '%s.jvm.mem.heap_used_percent': stat('gauge', 'nodes.%s.jvm.mem.heap_used_percent'),
  '%s.jvm.mem.heap_committed': stat('gauge', 'nodes.%s.jvm.mem.heap_committed_in_bytes'),
  '%s.jvm.mem.non_heap_used': stat('gauge', 'nodes.%s.jvm.mem.non_heap_used_in_bytes'),
  '%s.jvm.mem.non_heap_committed': stat('gauge', 'nodes.%s.jvm.mem.non_heap_committed_in_bytes'),

  # Pools:
  '%s.jvm.mem.survivor_used': stat('gauge', 'nodes.%s.jvm.mem.pools.survivor.used_in_bytes'),
  '%s.jvm.mem.survivor_peak_used': stat('gauge', 'nodes.%s.jvm.mem.pools.survivor.peak_used_in_bytes'),
  '%s.jvm.mem.survivor_max': stat('gauge', 'nodes.%s.jvm.mem.pools.survivor.max_in_bytes'),
  '%s.jvm.mem.survivor_peak_max': stat('gauge', 'nodes.%s.jvm.mem.pools.survivor.peak_max_in_bytes'),

  '%s.jvm.mem.old_used': stat('gauge', 'nodes.%s.jvm.mem.pools.old.used_in_bytes'),
  '%s.jvm.mem.old_peak_used': stat('gauge', 'nodes.%s.jvm.mem.pools.old.peak_used_in_bytes'),
  '%s.jvm.mem.old_max': stat('gauge', 'nodes.%s.jvm.mem.pools.old.max_in_bytes'),
  '%s.jvm.mem.old_peak_max': stat('gauge', 'nodes.%s.jvm.mem.pools.old.peak_max_in_bytes'),

  '%s.jvm.mem.young_used': stat('gauge', 'nodes.%s.jvm.mem.pools.young.used_in_bytes'),
  '%s.jvm.mem.young_peak_used': stat('gauge', 'nodes.%s.jvm.mem.pools.young.peak_used_in_bytes'),
  '%s.jvm.mem.young_max': stat('gauge', 'nodes.%s.jvm.mem.pools.young.max_in_bytes'),
  '%s.jvm.mem.young_peak_max': stat('gauge', 'nodes.%s.jvm.mem.pools.young.peak_max_in_bytes'),

  # GC:
  '%s.jvm.gc.old_time': stat('counter', 'nodes.%s.jvm.gc.collectors.old.collection_time_in_millis'),
  '%s.jvm.gc.old_count': stat('counter', 'nodes.%s.jvm.gc.collectors.old.collection_count'),
  '%s.jvm.gc.young_time': stat('counter', 'nodes.%s.jvm.gc.collectors.young.collection_time_in_millis'),
  '%s.jvm.gc.young_count': stat('counter', 'nodes.%s.jvm.gc.collectors.young.collection_count'),

  # Process:
  '%s.process.open_files': stat('gauge', 'nodes.%s.process.open_file_descriptors'),
  '%s.process.max_open_files': stat('gauge', 'nodes.%s.process.max_file_descriptors'),
  '%s.process.total_cpu_time': stat('gauge', 'nodes.%s.process.cpu.total_in_millis'),
  '%s.process.total_virtual_memory': stat('gauge', 'nodes.%s.process.mem.total_virtual_in_bytes'),

  # Indeces
  '%s.indeces.index_total': stat('counter', 'nodes.%s.indices.indexing.index_total'),
  '%s.indeces.index_current': stat('gauge', 'nodes.%s.indices.indexing.index_current'),
  '%s.indeces.index_failed': stat('counter', 'nodes.%s.indices.indexing.index_failed'),
  '%s.indeces.index_time': stat('counter', 'nodes.%s.indices.indexing.index_time_in_millis'),

  '%s.indeces.delete_total': stat('counter', 'nodes.%s.indices.indexing.delete_total'),
  '%s.indeces.delete_current': stat('gauge', 'nodes.%s.indices.indexing.delete_current'),
  '%s.indeces.delete_time': stat('counter', 'nodes.%s.indices.indexing.delete_time_in_millis'),

  '%s.indeces.query_total': stat('counter', 'nodes.%s.indices.search.query_total'),
  '%s.indeces.query_current': stat('gauge', 'nodes.%s.indices.search.query_current'),
  '%s.indeces.query_time': stat('counter', 'nodes.%s.indices.search.query_time_in_millis'),
  
  '%s.indeces.fetch_total': stat('counter', 'nodes.%s.indices.search.fetch_total'),
  '%s.indeces.fetch_current': stat('gauge', 'nodes.%s.indices.search.fetch_current'),
  '%s.indeces.fetch_time': stat('counter', 'nodes.%s.indices.search.fetch_time_in_millis'),

  '%s.indeces.merges_total': stat('counter', 'nodes.%s.indices.merges.total'),
  '%s.indeces.merges_current': stat('gauge', 'nodes.%s.indices.merges.current'),
  '%s.indeces.merges_time': stat('counter', 'nodes.%s.indices.merges.total_time_in_millis'),

  '%s.indeces.refresh': stat('counter', 'nodes.%s.indices.refresh.total'),
  '%s.indeces.refresh_time': stat('counter', 'nodes.%s.indices.refresh.total_time_in_millis'),

  '%s.indeces.translog': stat('counter', 'nodes.%s.indices.translog.operations'),
  '%s.indeces.translog_size': stat('gauge', 'nodes.%s.indices.translog.size_in_bytes'),
  '%s.indeces.translog_uncommitted': stat('gauge', 'nodes.%s.indices.translog.uncommitted_operations'),
  '%s.indeces.translog_uncommitted_size': stat('gauge', 'nodes.%s.indices.translog.uncommitted_size_in_bytes'),

  # Transport
  '%s.transport.server_open': stat('gauge', 'nodes.%s.transport.server_open'),
  '%s.transport.rx_count': stat('counter', 'nodes.%s.transport.rx_count'),
  '%s.transport.rx_size': stat('counter', 'nodes.%s.transport.rx_size_in_bytes'),
  '%s.transport.tx_count': stat('counter', 'nodes.%s.transport.tx_count'),
  '%s.transport.tx_size': stat('counter', 'nodes.%s.transport.tx_size_in_bytes'),

  # HTTP
  '%s.http.open': stat('gauge', 'nodes.%s.http.current_open'),
  '%s.http.total_opened': stat('gauge', 'nodes.%s.http.total_opened'),
}

# Thread pools
for thread_pool in ["bulk", "fetch_shard_started", "fetch_shard_store", "flush", "force_merge", "generic", "get", "index", "listener", "management", "refresh", "search", "snapshot", "warmer"]:
  STATS['%s.thread_pool.'+thread_pool+'.threads'] = stat('gauge', 'nodes.%s.thread_pool.'+thread_pool+'.threads')
  STATS['%s.thread_pool.'+thread_pool+'.queue'] = stat('gauge', 'nodes.%s.thread_pool.'+thread_pool+'.queue')
  STATS['%s.thread_pool.'+thread_pool+'.active'] = stat('gauge', 'nodes.%s.thread_pool.'+thread_pool+'.active')
  STATS['%s.thread_pool.'+thread_pool+'.rejected'] = stat('counter', 'nodes.%s.thread_pool.'+thread_pool+'.rejected')
  STATS['%s.thread_pool.'+thread_pool+'.largest'] = stat('gauge', 'nodes.%s.thread_pool.'+thread_pool+'.largest')
  STATS['%s.thread_pool.'+thread_pool+'.completed'] = stat('counter', 'nodes.%s.thread_pool.'+thread_pool+'.completed')

def fetch_stats():
  global CONFIGS
  if not CONFIGS: CONFIGS = CONFIG_DEFAULT
  for config in CONFIGS:
    try:
      stats = json.load(urllib2.urlopen(config["url"], timeout=config["timeout"]))
    except Exception as err:
      collectd.error("Logstash plugin ("+config["node"]+"): Error fetching stats from "+config["url"]+": "+str(err))
      return None
    parse_stats(stats, config)

def parse_stats(json, config):
  nodes = json['nodes'].keys()
  for node in nodes:
    es_node = reduce(lambda x, y: x[y], ["nodes", node, "name"], json).replace(".", "_")
    for name, stat in STATS.iteritems():
      path = (STATS[name].path % node).split('.')
      try:
        value = reduce(lambda x, y: x[y], path, json)
      except Exception as err:
        collectd.warning("Logstash plugin ("+config["node"]+"): Could not process path "+STATS[name].path+": "+str(err))
        continue
      dispatch_stat(name % es_node, value, stat.type, config)

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
    timeout = CONFIG_DEFAULT[0]["timeout"]
    if config.key == "Host": host = str(config.values[0])
    elif config.key == "Port": port = str(int(config.values[0]))
    elif config.key == "Name": node = str(config.values[0])
    elif config.key == "Timeout": timeout = int(config.values[0])
    else: collectd.warning("Logstash plugin: Unknown config key "+config.key)
    CONFIGS.append({
      "host": host,
      "port": port,
      "node": node,
      "url": "http://"+host+":"+port+"/_node/stats",
      "timeout": timeout
    })

collectd.register_config(config_callback)
collectd.register_read(read_callback)
