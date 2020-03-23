import matplotlib.pyplot as plt
from statistics import mean

# get average bs_lat(bs_lat is the latency in bluestore)
bs_lat_list = [x[1] for x in bs_lat_vec]
total_lat_list = [x[1] for x in total_lat_vec]

priInv = 0 # number of priority inversion
txn_numbers = len(bs_lat_vec)
for i in range(txn_numbers - 1):
    if bs_lat_vec[i][0] < bs_lat_vec[i+1][0]:
        priInv = priInv + 1
print("number of priority inversion =",priInv, "number of txcs =", txn_numbers)

fig, ax = plt.subplots()
#ax.set_yscale('log')
ax.plot(blocking_dur_vec, 'b-')
ax.set(xlabel='time', ylabel='blocking_duration[us]', title='blocking duration over time')

print("average bluestore latency(s) =",mean(bs_lat_list)/1000000)
print("average total latency(s) =",mean(total_lat_list)/1000000)

print("average osd queue size =",mean(osd_queue_size_vec))
fig, ax = plt.subplots()
ax.plot(osd_queue_size_vec, 'r-')
ax.set_ylim(0,14)
ax.set(xlabel='time', ylabel='osd_queue size', title='osd queue size')

print("average kv queue size =",mean(kv_queue_size_vec))
fig, ax = plt.subplots()
ax.plot(kv_queue_size_vec, 'c-')
ax.set(xlabel='time', ylabel='kv_queue size', title='bluestore kv queue size')