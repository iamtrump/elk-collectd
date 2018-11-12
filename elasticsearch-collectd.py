#!/usr/bin/env python

import collectd
import collections
import json
import urllib2

CONFIGS = []
CONFIG_DEFAULT = [{
  "host": "localhost",
  "port": "9200",
  "node": "elasticsearch",
  "url_nodes_stats": "http://localhost:9200/_nodes/stats",
  "url_cluster_health": "http://localhost:9200/_cluster/health",
  "url_cluster_stats": "http://localhost:9200/_cluster/stats",
  "stats": ["nodes_stats", "cluster_health", "cluster_stats"],
  "timeout": 20
}]

stat = collections.namedtuple("Stat", ("type", "path"))

# Metrics dictionary
STATS_NODES_STATS = {
  # Threads
  "nodes.%s.jvm.threads.count": stat("gauge", "nodes/%s/jvm/threads/count"),
  "nodes.%s.jvm.threads.peak_count": stat("gauge", "nodes/%s/jvm/threads/peak_count"),

  # Memory:
  "nodes.%s.jvm.mem.heap_max": stat("gauge", "nodes/%s/jvm/mem/heap_max_in_bytes"),
  "nodes.%s.jvm.mem.heap_used": stat("gauge", "nodes/%s/jvm/mem/heap_used_in_bytes"),
  "nodes.%s.jvm.mem.heap_used_percent": stat("gauge", "nodes/%s/jvm/mem/heap_used_percent"),
  "nodes.%s.jvm.mem.heap_committed": stat("gauge", "nodes/%s/jvm/mem/heap_committed_in_bytes"),
  "nodes.%s.jvm.mem.non_heap_used": stat("gauge", "nodes/%s/jvm/mem/non_heap_used_in_bytes"),
  "nodes.%s.jvm.mem.non_heap_committed": stat("gauge", "nodes/%s/jvm/mem/non_heap_committed_in_bytes"),

  # Pools:
  "nodes.%s.jvm.mem.survivor_used": stat("gauge", "nodes/%s/jvm/mem/pools/survivor/used_in_bytes"),
  "nodes.%s.jvm.mem.survivor_peak_used": stat("gauge", "nodes/%s/jvm/mem/pools/survivor/peak_used_in_bytes"),
  "nodes.%s.jvm.mem.survivor_max": stat("gauge", "nodes/%s/jvm/mem/pools/survivor/max_in_bytes"),
  "nodes.%s.jvm.mem.survivor_peak_max": stat("gauge", "nodes/%s/jvm/mem/pools/survivor/peak_max_in_bytes"),

  "nodes.%s.jvm.mem.old_used": stat("gauge", "nodes/%s/jvm/mem/pools/old/used_in_bytes"),
  "nodes.%s.jvm.mem.old_peak_used": stat("gauge", "nodes/%s/jvm/mem/pools/old/peak_used_in_bytes"),
  "nodes.%s.jvm.mem.old_max": stat("gauge", "nodes/%s/jvm/mem/pools/old/max_in_bytes"),
  "nodes.%s.jvm.mem.old_peak_max": stat("gauge", "nodes/%s/jvm/mem/pools/old/peak_max_in_bytes"),

  "nodes.%s.jvm.mem.young_used": stat("gauge", "nodes/%s/jvm/mem/pools/young/used_in_bytes"),
  "nodes.%s.jvm.mem.young_peak_used": stat("gauge", "nodes/%s/jvm/mem/pools/young/peak_used_in_bytes"),
  "nodes.%s.jvm.mem.young_max": stat("gauge", "nodes/%s/jvm/mem/pools/young/max_in_bytes"),
  "nodes.%s.jvm.mem.young_peak_max": stat("gauge", "nodes/%s/jvm/mem/pools/young/peak_max_in_bytes"),

  # GC:
  "nodes.%s.jvm.gc.old_time": stat("counter", "nodes/%s/jvm/gc/collectors/old/collection_time_in_millis"),
  "nodes.%s.jvm.gc.old_count": stat("counter", "nodes/%s/jvm/gc/collectors/old/collection_count"),
  "nodes.%s.jvm.gc.young_time": stat("counter", "nodes/%s/jvm/gc/collectors/young/collection_time_in_millis"),
  "nodes.%s.jvm.gc.young_count": stat("counter", "nodes/%s/jvm/gc/collectors/young/collection_count"),

  # Process:
  "nodes.%s.process.open_files": stat("gauge", "nodes/%s/process/open_file_descriptors"),
  "nodes.%s.process.max_open_files": stat("gauge", "nodes/%s/process/max_file_descriptors"),
  "nodes.%s.process.total_cpu_time": stat("gauge", "nodes/%s/process/cpu/total_in_millis"),
  "nodes.%s.process.total_virtual_memory": stat("gauge", "nodes/%s/process/mem/total_virtual_in_bytes"),

  # Indices
  "nodes.%s.indices.index_total": stat("counter", "nodes/%s/indices/indexing/index_total"),
  "nodes.%s.indices.index_current": stat("gauge", "nodes/%s/indices/indexing/index_current"),
  "nodes.%s.indices.index_failed": stat("counter", "nodes/%s/indices/indexing/index_failed"),
  "nodes.%s.indices.index_time": stat("counter", "nodes/%s/indices/indexing/index_time_in_millis"),

  "nodes.%s.indices.delete_total": stat("counter", "nodes/%s/indices/indexing/delete_total"),
  "nodes.%s.indices.delete_current": stat("gauge", "nodes/%s/indices/indexing/delete_current"),
  "nodes.%s.indices.delete_time": stat("counter", "nodes/%s/indices/indexing/delete_time_in_millis"),

  "nodes.%s.indices.query_total": stat("counter", "nodes/%s/indices/search/query_total"),
  "nodes.%s.indices.query_current": stat("gauge", "nodes/%s/indices/search/query_current"),
  "nodes.%s.indices.query_time": stat("counter", "nodes/%s/indices/search/query_time_in_millis"),
  
  "nodes.%s.indices.fetch_total": stat("counter", "nodes/%s/indices/search/fetch_total"),
  "nodes.%s.indices.fetch_current": stat("gauge", "nodes/%s/indices/search/fetch_current"),
  "nodes.%s.indices.fetch_time": stat("counter", "nodes/%s/indices/search/fetch_time_in_millis"),

  "nodes.%s.indices.merges_total": stat("counter", "nodes/%s/indices/merges/total"),
  "nodes.%s.indices.merges_current": stat("gauge", "nodes/%s/indices/merges/current"),
  "nodes.%s.indices.merges_time": stat("counter", "nodes/%s/indices/merges/total_time_in_millis"),

  "nodes.%s.indices.refresh": stat("counter", "nodes/%s/indices/refresh/total"),
  "nodes.%s.indices.refresh_time": stat("counter", "nodes/%s/indices/refresh/total_time_in_millis"),

  "nodes.%s.indices.translog": stat("counter", "nodes/%s/indices/translog/operations"),
  "nodes.%s.indices.translog_size": stat("gauge", "nodes/%s/indices/translog/size_in_bytes"),
  "nodes.%s.indices.translog_uncommitted": stat("gauge", "nodes/%s/indices/translog/uncommitted_operations"),
  "nodes.%s.indices.translog_uncommitted_size": stat("gauge", "nodes/%s/indices/translog/uncommitted_size_in_bytes"),

  # Transport
  "nodes.%s.transport.server_open": stat("gauge", "nodes/%s/transport/server_open"),
  "nodes.%s.transport.rx_count": stat("counter", "nodes/%s/transport/rx_count"),
  "nodes.%s.transport.rx_size": stat("counter", "nodes/%s/transport/rx_size_in_bytes"),
  "nodes.%s.transport.tx_count": stat("counter", "nodes/%s/transport/tx_count"),
  "nodes.%s.transport.tx_size": stat("counter", "nodes/%s/transport/tx_size_in_bytes"),

  # HTTP
  "nodes.%s.http.open": stat("gauge", "nodes/%s/http/current_open"),
  "nodes.%s.http.total_opened": stat("gauge", "nodes/%s/http/total_opened"),
}

# Thread pools
for thread_pool in ["bulk", "fetch_shard_started", "fetch_shard_store", "flush", "force_merge", "generic", "get", "index", "listener", "management", "refresh", "search", "snapshot", "warmer"]:
  STATS_NODES_STATS["nodes.%s.thread_pool."+thread_pool+".threads"] = stat("gauge", "nodes/%s/thread_pool/"+thread_pool+"/threads")
  STATS_NODES_STATS["nodes.%s.thread_pool."+thread_pool+".queue"] = stat("gauge", "nodes/%s/thread_pool/"+thread_pool+"/queue")
  STATS_NODES_STATS["nodes.%s.thread_pool."+thread_pool+".active"] = stat("gauge", "nodes/%s/thread_pool/"+thread_pool+"/active")
  STATS_NODES_STATS["nodes.%s.thread_pool."+thread_pool+".rejected"] = stat("counter", "nodes/%s/thread_pool/"+thread_pool+"/rejected")
  STATS_NODES_STATS["nodes.%s.thread_pool."+thread_pool+".largest"] = stat("gauge", "nodes/%s/thread_pool/"+thread_pool+"/largest")
  STATS_NODES_STATS["nodes.%s.thread_pool."+thread_pool+".completed"] = stat("counter", "nodes/%s/thread_pool/"+thread_pool+"/completed")

# Breakers
for breaker in ["accounting", "fielddata", "in_flight_requests", "parent", "request"]:
  STATS_NODES_STATS["nodes.%s.breakers."+breaker+".estimated_size"] = stat("gauge", "nodes/%s/breakers/"+breaker+"/estimated_size_in_bytes")
  STATS_NODES_STATS["nodes.%s.breakers."+breaker+".limit_size"] = stat("gauge", "nodes/%s/breakers/"+breaker+"/limit_size_in_bytes")
  STATS_NODES_STATS["nodes.%s.breakers."+breaker+".overhead"] = stat("gauge", "nodes/%s/breakers/"+breaker+"/overhead")
  STATS_NODES_STATS["nodes.%s.breakers."+breaker+".tripped"] = stat("counter", "nodes/%s/breakers/"+breaker+"/tripped")

# Cluster health stats
STATS_CLUSTER_HEALTH = {
  "cluster.nodes.number_of_nodes": stat("gauge", "number_of_nodes"),
  "cluster.nodes.number_of_data_nodes": stat("gauge", "number_of_data_nodes"),
  "cluster.shards.active_primary": stat("gauge", "active_primary_shards"),
  "cluster.shards.active": stat("gauge", "active_shards"),
  "cluster.shards.relocating": stat("gauge", "relocating_shards"),
  "cluster.shards.initializing": stat("gauge", "initializing_shards"),
  "cluster.shards.unassigned": stat("gauge", "unassigned_shards"),
  "cluster.shards.delayed_unassigned": stat("gauge", "delayed_unassigned_shards"),
  "cluster.number_of_pending_tasks": stat("gauge", "number_of_pending_tasks"),
  "cluster.number_of_in_flight_fetch": stat("gauge", "number_of_in_flight_fetch"),
  "cluster.task_max_waiting_in_queue": stat("gauge", "task_max_waiting_in_queue_millis"),
  "cluster.active_shards_percent": stat("gauge", "active_shards_percent_as_number"),
}

# Cluster stats
STATS_CLUSTER_STATS = {
  "fs.available": stat("gauge", "nodes/fs/available_in_bytes"),
  "fs.total": stat("gauge", "nodes/fs/total_in_bytes"),
  "indices.count": stat("gauge", "indices/count"),
  "indices.shards.total": stat("gauge", "indices/shards/total"),
  "indices.shards.primaries": stat("gauge", "indices/shards/primaries"),
  "indices.shards.replication": stat("gauge", "indices/shards/replication"),
  "indices.docs.count": stat("gauge", "indices/docs/count"),
  "indices.docs.deleted": stat("gauge", "indices/docs/deleted"),
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
    total_stats = dict()
    try:
      if "nodes_stats" in config["stats"]:
        total_stats["nodes_stats"] = json.load(urllib2.urlopen(config["url_nodes_stats"], timeout=config["timeout"]))
    except Exception as err:
      collectd.error("Elasticsearch plugin ("+config["node"]+"): Error fetching nodes stats from "+config["url_nodes_stats"]+": "+str(err))
      return None
    try:
      if "cluster_health" in config["stats"]:
        total_stats["cluster_health"] = json.load(urllib2.urlopen(config["url_cluster_health"], timeout=config["timeout"]))
    except Exception as err:
      collectd.error("Elasticsearch plugin ("+config["node"]+"): Error fetching cluster health from "+config["url_cluster_health"]+": "+str(err))
      return None
    try:
      if "cluster_stats" in config["stats"]:
        total_stats["cluster_stats"] = json.load(urllib2.urlopen(config["url_cluster_stats"], timeout=config["timeout"]))
    except Exception as err:
      collectd.error("Elasticsearch plugin ("+config["node"]+"): Error fetching cluster stats from "+config["url_cluster_stats"]+": "+str(err))
      return None
    parse_stats(total_stats, config)

def parse_stats(json, config):
  if "nodes_stats" in config["stats"]:
    nodes = json["nodes_stats"]["nodes"].keys()
    for node in nodes:
      es_node = extract_value(json["nodes_stats"], "nodes/"+node+"/name").replace(".", "_")
      for name, stat in STATS_NODES_STATS.iteritems():
        try:
          value = extract_value(json["nodes_stats"], STATS_NODES_STATS[name].path % node)
        except Exception as err:
          collectd.warning("Elasticsearch plugin ("+config["node"]+"): Could not process path "+STATS_NODES_STATS[name].path+": "+str(err))
          continue
        dispatch_stat(name % es_node, value, stat.type, config)
  if "cluster_health" in config["stats"]:
    for name, stat in STATS_CLUSTER_HEALTH.iteritems():
      try:
        value = extract_value(json["cluster_health"], STATS_CLUSTER_HEALTH[name].path)
      except Exception as err:
        collectd.warning("Elasticsearch plugin ("+config["node"]+"): Could not process path "+STATS_CLUSTER_HEALTH[name].path+": "+str(err))
        continue
      dispatch_stat(name, value, stat.type, config)
  if "cluster_stats" in config["stats"]:
    for name, stat in STATS_CLUSTER_STATS.iteritems():
      try:
        value = extract_value(json["cluster_stats"], STATS_CLUSTER_STATS[name].path)
      except Exception as err:
        collectd.warning("Elasticsearch plugin ("+config["node"]+"): Could not process path "+STATS_CLUSTER_STATS[name].path+": "+str(err))
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
    stats = CONFIG_DEFAULT[0]["stats"]
    timeout = CONFIG_DEFAULT[0]["timeout"]
    if config.key == "Host": host = str(config.values[0])
    elif config.key == "Port": port = str(int(config.values[0]))
    elif config.key == "Name": node = str(config.values[0])
    elif config.key == "Timeout": timeout = int(config.values[0])
    elif config.key == "Stats": stats = config.values[0].split(" ")
    else: collectd.warning("Elasticsearch plugin: Unknown config key "+config.key)
    CONFIGS.append({
      "host": host,
      "port": port,
      "node": node,
      "url_nodes": "http://"+host+":"+port+"/_node/stats",
      "url_cluster_health": "http://"+host+":"+port+"/_cluster/health",
      "url_cluster_stats": "http://"+host+":"+port+"/_cluster/stats",
      "stats": stats,
      "timeout": timeout
    })

collectd.register_config(config_callback)
collectd.register_read(read_callback)
