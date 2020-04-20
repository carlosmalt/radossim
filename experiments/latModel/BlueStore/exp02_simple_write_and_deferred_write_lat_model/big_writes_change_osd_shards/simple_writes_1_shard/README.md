## simple writes dominate

```
workload: 
bs = 4096
type =randwrite
tool = fio
```

```
        osd_op_num_shards = 1
        osd_op_num_threads_per_shard = 2
        enable_throttle = false
        enable_codel = false
        enable_batch_bound = false
        kv_queue_upper_bound_size = 30
        bluestore_throttle_bytes = 100000000
        bluestore_throttle_deferred_bytes = 100000000
        bluestore_throttle_cost_per_io = 0
        bluestore_throttle_cost_per_io_hdd = 100000
        bluestore_throttle_cost_per_io_ssd = 100000
        bdev_block_size = 4096
        bluestore_min_alloc_size = 0
        bluestore_min_alloc_size_hdd = 65536
        bluestore_min_alloc_size_ssd =  4096
        bluestore_max_alloc_size = 0
        bluestore_prefer_deferred_size = 0
        bluestore_prefer_deferred_size_hdd = 0
        bluestore_prefer_deferred_size_ssd = 0
```