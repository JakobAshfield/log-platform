from prometheus_client import Counter, Histogram

log_ingest_counter = Counter(
    "log_ingest_total",
    "Total log entries ingested",
    ["level"]
)

cache_hit_counter = Counter("cache_hits_total", "Redis cache hits")
cache_miss_counter = Counter("cache_misses_total", "Redis cache misses")