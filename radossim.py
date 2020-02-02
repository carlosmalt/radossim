# Simulate the interaction between osd queues and kv queue that causes
# bufferbloat

import simpy
import random
import math

# Predict request process time in micro seconds (roughly based on spinning media model
# kaldewey:rtas08, Fig 2)
def latModel(reqSize):
    reqSize /= 4096.0
    if reqSize > 16:
            iops = 820.28 * math.log(reqSize) - 1114.3
    else:
            iops = 62.36 * reqSize + 8.33
    #print(int(1000000 / iops))
    return int(1000000 / iops)

# Create requests with a fixed priority and certain inter-arrival and size distributions
def osdClient(env, priority, meanInterArrivalTime, meanReqSize, dstQ):
    while True:
        # Wait until arrival time
        yield env.timeout(random.expovariate(1.0/meanInterArrivalTime))
        # Assemble request and timestamp
        request = (priority, random.expovariate(1.0/meanReqSize), env.now)
        # Submit request
        with dstQ.put(request) as put:
            yield put

# Move requests to BlueStore
def osdThread(env, srcQ, dstQ):
    while True:
        # Wait until there is something in the srcQ
        with srcQ.get() as get:
            osdRequest = yield get
            # Timestamp transaction (after this time request cannot be prioritized)
            bsTxn = (osdRequest, env.now)
        # Submit BlueStore transaction
        with dstQ.put(bsTxn) as put:
            yield put

# Batch incoming requests and process
def kvThread(env, srcQ):
    bm = BatchManagement()
    while True:
        # Create batch
        batch = []
        # Build request of entire batch (see Issue #6)
        batchReqSize = 0
        # Wait until there is something in the srcQ
        with srcQ.get() as get:
            bsTxn = yield get
            batch.append(bsTxn)
            # Unpack transaction
            ((priority, reqSize, arrivalOSD), arrivalKV) = bsTxn
            batchReqSize += reqSize
        # Batch everything that is now in srcQ
        # batch size is governed by srcQ.capacity
        if srcQ.capacity == float('inf'):
            batchSize = len(srcQ.items)
        else:
            batchSize = min(srcQ.capacity-1, len(srcQ.items))
        # Do batch
        for i in range(batchSize):
            with srcQ.get() as get:
                bsTxn = yield get
                batch.append(bsTxn)
                # Unpack transaction
                ((priority, reqSize, arrivalOSD), arrivalKV) = bsTxn
                batchReqSize += reqSize
        # Process batch
        kvQDispatch = env.now
        yield env.timeout(latModel(batchReqSize))
        kvCommit = env.now
        # Diagnose and manage batching
        bm.processBatch(batch, batchReqSize, kvQDispatch, kvCommit)

# Manage batch sizing
class BatchManagement:
    def __init__(self):
        self.latMap = {}; self.cntMap = {}; self.count = 0; self.lat = 0

    def processBatch(self, batch, batchSize, dispatchTime, commitTime):
        for txn in batch:
            self.printLats(txn, dispatchTime)

    def printLats(self, txn, dispatchTime):
        ((priority, reqSize, arrivalOSD), arrivalKV) = txn
        # Account latencies
        osdQLat = arrivalKV - arrivalOSD
        kvQLat = dispatchTime - arrivalKV
        self.count += 1
        self.lat += osdQLat + kvQLat
        if priority in self.latMap:
            self.latMap[priority] += osdQLat + kvQLat
            self.cntMap[priority] += 1
        else:
            self.latMap[priority] = osdQLat + kvQLat
            self.cntMap[priority] = 1
        # Periodically print latencies (averages so far)
        if self.count % 10000 == 0:
            for priority in self.latMap.keys():
                print(priority, self.latMap[priority] / self.cntMap[priority] / 1000000)
            print('total', self.lat / self.count / 1000000)

if __name__ == '__main__':

        env = simpy.Environment()

        # Constants
        meanInterArrivalTime = 28500 # micro seconds
        meanReqSize = 4096 # bytes
        #meanInterArrivalTime = 4200 # micro seconds
        #meanReqSize = 16 * 4096 # bytes

        # OSD queue(s)
        # Add capacity parameter for max queue lengths
        osdQ1 = simpy.PriorityStore(env)
        osdQ2 = simpy.PriorityStore(env)
        #osdQ = simpy.Store(env) # infinite capacity

        # KV queue (capacity translates into batch size)
        kvQ = simpy.Store(env, 1)

        # OSD client(s), each with a particular priority pushing request into a particular queue
        env.process(osdClient(env, 1, meanInterArrivalTime*2, meanReqSize, osdQ1))
        env.process(osdClient(env, 2, meanInterArrivalTime*2, meanReqSize, osdQ1))

        # OSD thread(s) (one per OSD queue)
        # env.process(osdThread(env, osdQ, kvQ))
        env.process(osdThread(env, osdQ1, kvQ))
        env.process(osdThread(env, osdQ2, kvQ))

        # KV thread in BlueStore
        env.process(kvThread(env, kvQ))

        # Run simulation
        env.run(60 * 60 * 1000000)
