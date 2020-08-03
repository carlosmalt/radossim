# Simulate the interaction between osd queues and kv queue that causes
# bufferbloat

import simpy
from functools import partial, wraps
import math
import argparse
from latency_model import StatisticLatencyModel
from workload import OsdClientBench4K, RandomOSDClient, OsdClientBenchConstantSize
import pickle

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
        batch = []
        with srcQ.get() as get:
            req = yield get
            req = (req, env.now)
            batch.append(req)
        qDepth = 48
        for i in range(min(qDepth - 1, len(srcQ.items))):
            with srcQ.get() as get:
                req = yield get
                req = (req, env.now)
                batch.append(req)
        # Timestamp transaction (after this time request cannot be prioritized)
        req = batch
        # Submit BlueStore transaction
        with dstQ.put(req) as put:
            yield put


# def aioThread(env, srcQ, dstQ, latencyModel):
#     while True:
#         batch = []
#         with srcQ.get() as get:
#             req = yield get
#             batch.append(req)
#         qDepth = 48
#         for i in range(min(qDepth - 1, len(srcQ.items))):
#             with srcQ.get() as get:
#                 req = yield get
#                 batch.append(req)
#         print(len(batch))
#         aioSubmit = env.now
#         ((_, reqSize, _), _) = req
#         yield env.timeout(latencyModel.submitAIO(reqSize))
#         aioDone = env.now
#         for req in batch:
#             req = (req, aioSubmit, aioDone)
#             with dstQ.put(req) as put:
#                 yield put
#
#
# def kvThread(env, srcQ, latencyModel, targetLat=5000, measInterval=100000, data=None):
#     while True:
#         # Create batch
#         batch = []
#         batchReqSize = 0
#         # Wait for the first request
#         with srcQ.get() as get:
#             bsTxn = yield get
#             batch.append(bsTxn)
#             # Unpack transaction
#             (((priority, reqSize, arrivalOSD), arrivalKV), aioSubmit, aioDone) = bsTxn
#             batchReqSize += reqSize
#         # Drain the kvQueue
#         # print(len(srcQ.items))
#         for i in range(len(srcQ.items)):
#             with srcQ.get() as get:
#                 bsTxn = yield get
#                 batch.append(bsTxn)
#                 # Unpack transaction
#                 (((priority, reqSize, arrivalOSD), arrivalKV), aioSubmit, aioDone) = bsTxn
#                 batchReqSize += reqSize
#         # Process batch
#         kvQDispatch = env.now
#         if len(batch) > 48:
#             print(f'batch: {len(batch)}')
#         yield env.timeout(latencyModel.applyWrite(batchReqSize))
#         kvCommit = env.now
#         # print(kvCommit)
#         if data is not None:
#             for req in batch:
#                 req = (req, kvQDispatch, kvCommit)
#                 data.append(req)


def kvAndAioThread(env, srcQ, latencyModel, targetLat=5000, measInterval=100000, data=None):
    while True:
        # Create batch
        batchReqSize = 0
        batch = []
        with srcQ.get() as get:
            batch = yield get
            for req in batch:
                ((_, reqSize, _), _) = req
                batchReqSize += reqSize
        if len(batch) > 48:
            print(f'aio batch: {len(batch)}')
        aioSubmit = env.now
        yield env.timeout(latencyModel.submitAIO(batchReqSize))
        aioDone = env.now
        kvBatch = []
        for req in batch:
            req = (req, aioSubmit, aioDone)
            kvBatch.append(req)

        # Process batch
        kvQDispatch = env.now
        if len(kvBatch) > 48:
            print(f'kv batch: {len(kvBatch)}')
        latency = latencyModel.applyWrite(batchReqSize)
        yield env.timeout(latency)
        kvCommit = env.now
        # print(kvCommit)
        if data is not None:
            for req in kvBatch:
                req = (req, kvQDispatch, kvCommit)
                data.append(req)


# Batch incoming requests and process
def kvThreadOld(env, srcQ, latencyModel, targetLat=5000, measInterval=100000):
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


def runSimulation(model, targetLat=5000, measInterval=100000, time=5 * 60 * 1_000_000, output=None):
    def patchResource(resource, preCallback=None, postCallback=None):
        """Patch *resource* so that it calls the callable *preCallback* before each
        put/get/request/release operation and the callable *postCallback* after each
        operation.  The only argument to these functions is the resource
        instance.
        """

        def getWrapper(func):
            # Generate a wrapper for put/get/request/release
            @wraps(func)
            def wrapper(*args, **kwargs):
                # This is the actual wrapper
                # Call "pre" callback
                if preCallback:
                    preCallback(resource)

                # Perform actual operation
                ret = func(*args, **kwargs)

                # Call "post" callback
                if postCallback:
                    postCallback(resource)

                return ret

            return wrapper

        # Replace the original operations with our wrapper
        for name in ['put', 'get']:
            if hasattr(resource, name):
                setattr(resource, name, getWrapper(getattr(resource, name)))

    def monitor(data, resource):
        """Monitor queue len"""
        data.sum += len(resource.items)
        data.size += 1

    class QueueLenMonitor:
        def __init__(self):
            self.sum = 0
            self.size = 0

    env = simpy.Environment()

    # Constants
    # meanInterArrivalTime = 28500  # micro seconds
    # meanReqSize = 4096  # bytes
    # meanInterArrivalTime = 4200 # micro seconds
    # meanReqSize = 16 * 4096 # bytes
    latencyModel = StatisticLatencyModel(model)
    # OSD queue(s)
    # Add capacity parameter for max queue lengths
    queueDepth = 48
    osdQ1 = simpy.PriorityStore(env, capacity=queueDepth)
    # osdQ2 = simpy.PriorityStore(env, capacity=queueDepth)
    # osdQ = simpy.Store(env) # infinite capacity

    # monitoring
    queuLenMonitor = QueueLenMonitor()
    monitor = partial(monitor, queuLenMonitor)
    patchResource(osdQ1, postCallback=monitor)

    # KV queue (capacity translates into initial batch size)
    aioQ = simpy.Store(env, 1024)
    # kvQ = simpy.Store(env)

    # OSD client(s), each with a particular priority pushing request into a particular queue

    # random size and speed osd client
    # osdClientPriorityOne = RandomOSDClient(meanInterArrivalTime * 2, meanReqSize, 1, osdQ1)
    # osdClientPriorityTwo = RandomOSDClient(meanInterArrivalTime * 2, meanReqSize, 2, osdQ1)

    # 4k osd client workload generator
    osdClientPriorityOne = OsdClientBench4K(1)
    # osdClientPriorityTwo = OsdClientBench4K(2)

    env.process(osdClient(env, osdClientPriorityOne, osdQ1))
    # env.process(osdClient(env, osdClientPriorityTwo, osdQ1))

    # OSD thread(s) (one per OSD queue)
    # env.process(osdThread(env, osdQ, kvQ))
    env.process(osdThread(env, osdQ1, aioQ))
    # env.process(osdThread(env, osdQ2, kvQ))

    # AIO thread in BlueStore
    # env.process(aioThread(env, aioQ, kvQ, latencyModel))

    # KV thread in BlueStore with targetMinLat and measurement interval (in usec)
    data = None
    if output:
        data = []
    # env.process(kvThread(env, kvQ, latencyModel, targetLat, measInterval, data))
    env.process(kvAndAioThread(env, aioQ, latencyModel, targetLat, measInterval, data))

    # if outputFile:
    #     env.process(outputResults(env, outputQ, outputFile))

    # Run simulation
    env.run(time)
    if output:
        with open(output, 'wb') as f:
            pickle.dump(data, f)
    duration = env.now / 1_000_000  # to sec
    bytesWritten = latencyModel.bytesWritten
    avgThrouput = bytesWritten / duration

    return avgThrouput, queuLenMonitor.sum / queuLenMonitor.size


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simulate Ceph/RADOS.')
    parser.add_argument('--model',
                        metavar='filepath',
                        required=False,
                        default='latency_model.yaml',
                        help='filepath of latency model (default "latency_model_4K.yaml")'
                        )
    parser.add_argument('--output',
                        metavar='output path',
                        required=False,
                        default=None,
                        help='filepath of output for storing the results (default: No output)'
                        )

    args = parser.parse_args()
    targetLat = 5000
    measInterval = 100000
    time = 5 * 60 * 1_000_000   # 5 mins
    (avgBandwidth, avgOsdQueueLen) = runSimulation(args.model, targetLat, measInterval, time, args.output)
    avgBandwidth = avgBandwidth / 1024
    print(f'Bandwidth: {avgBandwidth} KB/s')
    print(f'OSD Queue Len: {avgOsdQueueLen}')
