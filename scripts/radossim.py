# Simulate the interaction between osd queues and kv queue that causes
# bufferbloat

import simpy
import random
from scipy.stats import nct
import functools
import math
from latency_model import LatencyModel4K
from workload import OsdClientBench4K, RandomOSDClient


# Create requests with a fixed priority and certain inter-arrival and size distributions
def osdClient(env, workloadGenerator, dstQ):
    while True:
        # Wait until arrival time
        timeout = workloadGenerator.calculateTimeout()
        if timeout > 0:
            yield env.timeout(timeout)
        # Assemble request and timestamp
        request = workloadGenerator.createRequest(env)
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
def kvThread(env, srcQ, latencyModel, targetLat=5000, measInterval=100000):
    bm = BatchManagement(srcQ, targetLat, measInterval)
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
        # Batch everything that is now in srcQ or up to bm.batchSize
        # initial batch size is governed by srcQ.capacity
        # but then updated by BatchManagement
        if bm.batchSize == float("inf"):
            batchSize = len(srcQ.items)
        else:
            batchSize = int(min(bm.batchSize - 1, len(srcQ.items)))
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
        yield env.timeout(latencyModel.applyWrite(batchReqSize))
        kvCommit = env.now
        # Diagnose and manage batching
        bm.manageBatch(batch, batchReqSize, kvQDispatch, kvCommit)


# Manage batch sizing
class BatchManagement:
    def __init__(self, queue, minLatTarget=5000, initInterval=100000):
        self.queue = queue
        # Latency state
        self.latMap = {}
        self.cntMap = {}
        self.count = 0
        self.lat = 0
        # Controlled Delay (CoDel) state
        self.minLatTarget = minLatTarget
        self.initInterval = self.interval = initInterval
        self.intervalStart = None
        self.minLatViolationCnt = 0
        self.intervalAdj = lambda x: math.sqrt(x)
        self.minLat = None
        # Batch sizing state
        self.batchSize = self.queue.capacity
        self.batchSizeInit = 100
        self.batchDownSize = lambda x: int(x / 2)
        self.batchUpSize = lambda x: int(x + 10)
        # written data state
        self.bytesWritten = 0

    def manageBatch(self, batch, batchSize, dispatchTime, commitTime):
        for txn in batch:
            ((priority, reqSize, arrivalOSD), arrivalKV) = txn
            # Account latencies
            osdQLat = arrivalKV - arrivalOSD
            kvQLat = dispatchTime - arrivalKV
            self.bytesWritten += reqSize
            self.count += 1
            self.lat += osdQLat + kvQLat
            if priority in self.latMap:
                self.latMap[priority] += osdQLat + kvQLat
                self.cntMap[priority] += 1
            else:
                self.latMap[priority] = osdQLat + kvQLat
                self.cntMap[priority] = 1
            self.fightBufferbloat(kvQLat, dispatchTime)
            self.printLats()

    # Implement CoDel algorithm and call batchSizing
    def fightBufferbloat(self, currQLat, currentTime):
        if not self.minLat or currQLat < self.minLat:
            self.minLat = currQLat
        if not self.intervalStart:
            self.intervalStart = currentTime
        elif currentTime - self.intervalStart >= self.interval:
            # Time to check on minimum latency
            if self.minLat > self.minLatTarget:
                # Minimum latency violation
                self.minLatViolationCnt += 1
                self.interval = self.initInterval / self.intervalAdj(
                    self.minLatViolationCnt
                )
                # Call batchSizing to downsize batch
                self.batchSizing(True)
            else:
                # No violation: reset count and interval length
                self.minLatViolationCnt = 0
                self.interval = self.initInterval
                # Call batchSizing to upsize batch
                self.batchSizing(False)
            self.minLat = None
            self.intervalStart = currentTime

    def batchSizing(self, isTooLarge):
        if isTooLarge:
            print("batch size", self.batchSize, "is too large")
            if self.batchSize == float("inf"):
                self.batchSize = self.batchSizeInit
            else:
                self.batchSize = self.batchDownSize(self.batchSize)
            print("new batch size is", self.batchSize)
        elif self.batchSize != float("inf"):
            # print('batch size', self.batchSize, 'gets larger')
            self.batchSize = self.batchUpSize(self.batchSize)
            # print('new batch size is', self.batchSize)

    def printLats(self, freq=1000):
        if self.count % freq == 0:
            for priority in self.latMap.keys():
                print(priority, self.latMap[priority] / self.cntMap[priority] / 1000000)
            print("total", self.lat / self.count / 1000000)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description='Simulate Ceph/RADOS.')
    parser.add_argument('model',
        metavar='filepath',
        default='latency_model.yaml',
        help='filepath of latency model (default "latency_model.yaml")'
        )
    args = parser.parse_args()

    env = simpy.Environment()

    # Constants
    # meanInterArrivalTime = 28500  # micro seconds
    # meanReqSize = 4096  # bytes
    # meanInterArrivalTime = 4200 # micro seconds
    # meanReqSize = 16 * 4096 # bytes
    latencyModel = LatencyModel4K(args.model)
    # OSD queue(s)
    # Add capacity parameter for max queue lengths
    queueDepth = 48
    osdQ1 = simpy.PriorityStore(env, capacity=queueDepth)
    osdQ2 = simpy.PriorityStore(env, capacity=queueDepth)
    # osdQ = simpy.Store(env) # infinite capacity

    # KV queue (capacity translates into initial batch size)
    kvQ = simpy.Store(env, 1)

    # OSD client(s), each with a particular priority pushing request into a particular queue

    # random size and speed osd client
    # osdClientPriorityOne = RandomOSDClient(meanInterArrivalTime * 2, meanReqSize, 1, osdQ1)
    # osdClientPriorityTwo = RandomOSDClient(meanInterArrivalTime * 2, meanReqSize, 2, osdQ1)

    # 4k osd client workload generator
    osdClientPriorityOne = OsdClientBench4K(4096.0, 1)
    osdClientPriorityTwo = OsdClientBench4K(4096.0, 2)

    env.process(osdClient(env, osdClientPriorityOne, osdQ1))
    env.process(osdClient(env, osdClientPriorityTwo, osdQ1))

    # OSD thread(s) (one per OSD queue)
    # env.process(osdThread(env, osdQ, kvQ))
    env.process(osdThread(env, osdQ1, kvQ))
    env.process(osdThread(env, osdQ2, kvQ))

    # KV thread in BlueStore with targetMinLat and measurement interval (in usec)
    env.process(kvThread(env, kvQ, latencyModel, 80000, 1600000))

    # Run simulation
    env.run(5 * 60 * 1000000)
