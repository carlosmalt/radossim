## Simple Write(non-deferred) Model with Different `threads_per_shard`

In `exp01`, we set the `threads_per_shard = 1`, and in `exp02`, we set `threads_per_shard = 2`. From the results we can get these facts:

1. Throughput increase(from 32 to 50 to 54 MB/s)
2. Average BlueStore Latency increases slowly
3. Total latency decreases slowly
4. Average kv_queue size increases from 4 to 11 tp 15
5. Average op_queue size decreases from 27 to 17 to 14


Conclusion:
With more osd threads, the dispatching speed is increased. Thus, the op_queue size decireses and kv_queue increases. 