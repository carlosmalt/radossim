# Simulate the interaction between osd queues and kv queue that causes
# bufferbloat

import simpy
from functools import partial, wraps
import math
import argparse
from .latency_model import StatisticLatencyModel
from .workload import OsdClientBench4K, RandomOSDClient, OsdClientBenchConstantSize
from .simpy_utils import patchResource, VariableCapacityStore
import pickle
import matplotlib.pyplot as plt


# Create requests with a fixed priority and certain inter-arrival and size distributions
def osdClient(env, workloadGenerator, dstQ, ioDepth, ioDepthLockQueue):
    with ioDepthLockQueue.put(0) as put:
        yield put
    requestIndex = 0
    while True:
        # Wait until arrival time
        timeout = workloadGenerator.calculateTimeout()
        if timeout > 0:
            yield env.timeout(timeout)
        request = workloadGenerator.createRequest(env)
        requestIndex += 1
        if requestIndex < ioDepth:
            request = (request, False)
        else:
            request = (request, True)
            requestIndex = 0
        # Submit request
        with dstQ.put(request) as put:
            yield put
        if request[len(request) - 1]:
            with ioDepthLockQueue.put(0) as put:
                yield put



# Move requests to BlueStore
def osdThread(env, srcQ, dstQ):
    while True:
        # Wait until there is something in the srcQ
        with srcQ.get() as get:
            req = yield get
        with dstQ.put(req) as put:
            yield put


def kvAndAioThread(env, srcQ, latencyModel, batchManagement, ioDepthLockQueue, data=None, useCoDel=True):
    bs = batchManagement.batchSize
    while True:
        batchReqSize = 0
        batch = []
        with srcQ.get() as get:
            req = yield get
            (((_, reqSize, _), _), arrivalKV) = req
            batchReqSize += reqSize
            batch.append(req)
        batchSize = int(min(len(srcQ.items), 1023))
        for i in range(batchSize):
            with srcQ.get() as get:
                req = yield get
                (((_, reqSize, _), _), arrivalKV) = req
                batchReqSize += reqSize
                batch.append(req)
        # print(len(batch))
        aioSubmit = env.now
        # timeout = latencyModel.submitAIO(batchReqSize)
        # yield env.timeout(timeout)
        aioDone = env.now
        kvBatch = []
        for req in batch:
            req = (req, aioSubmit, aioDone)
            kvBatch.append(req)


        # Process batch
        kvQDispatch = env.now
        latency = latencyModel.applyWrite(batchReqSize)
        yield env.timeout(latency)
        kvCommit = env.now

        for req in kvBatch:
            ((((_, _, _), releaseIoQueue), _), _, _) = req
            if releaseIoQueue:
                with ioDepthLockQueue.get() as get:     # release lock on ioDepth
                    _ = yield get
            if data is not None:
                req = (req, kvQDispatch, kvCommit)
                data.append(req)
        if useCoDel:
            batchManagement.manageBatch(kvBatch, batchReqSize, kvQDispatch, kvCommit)
            # if batchManagement.batchSize != bs:
            #     bs = batchManagement.batchSize
            #     print(f'cap changed to {batchManagement.batchSize}')
            srcQ.changeCapacity(batchManagement.batchSize)


# Manage batch sizing
class BatchManagement:
    def __init__(self, queue, minLatTarget=5000, initInterval=100000, downSize=None, upSize=None, upSizeLimit=False):
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
        if downSize:
            self.batchDownSize = lambda x: int(x / downSize)
        else:
            self.batchDownSize = lambda x: int(x / 2)
        if upSize:
            self.batchUpSize = lambda x: int(x + upSize)
        else:
            self.batchUpSize = lambda x: int(x + 1)
        # written data state
        self.bytesWritten = 0
        self.maxQueueLen = 0
        self.batchSizeLog = []
        self.timeLog = []
        self.batchSizeLog.append(self.batchSizeInit)
        self.timeLog.append(0)
        self.upSizeLimit = upSizeLimit

    def manageBatch(self, batch, batchSize, dispatchTime, commitTime):
        for txn in batch:
            ((((priority, reqSize, arrivalOSD), _), arrivalKV), aioSubmit, aioDone) = txn
            # Account latencies
            osdQLat = arrivalKV - arrivalOSD
            kvQLat = dispatchTime - arrivalKV
            # print(kvQLat)
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
            # self.printLats()

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
            self.maxQueueLen = 0
            if self.batchSize != float("inf"):
                self.batchSizeLog.append(self.batchSize)
                self.timeLog.append(self.queue._env.now)
        else:
            if self.maxQueueLen < len(self.queue.items):
                self.maxQueueLen = len(self.queue.items)

    def batchSizing(self, isTooLarge):
        if isTooLarge:
            # print("batch size", self.batchSize, "is too large")
            if self.batchSize == float("inf"):
                self.batchSize = self.batchSizeInit
            else:
                self.batchSize = self.batchDownSize(self.batchSize)
                if self.batchSize == 0:
                    self.batchSize = 1
            # print("new batch size is", self.batchSize)
        elif self.batchSize != float("inf"):
            if not self.upSizeLimit or self.batchSize < 1.5 * self.maxQueueLen:
        # elif self.batchSize < 200:
            # print('batch size', self.batchSize, 'gets larger')
                self.batchSize = self.batchUpSize(self.batchSize)
            # print('new batch size is', self.batchSize)
            

    def printLats(self, freq=1000):
        if self.count % freq == 0:
            for priority in self.latMap.keys():
                print(priority, self.latMap[priority] / self.cntMap[priority] / 1000000)
            print("total", self.lat / self.count / 1000000)


class AdaptiveBatchManagement(BatchManagement):
    def batchSizing(self, isTooLarge):
        if isTooLarge:
            alpha = 0.3
        else:
            alpha = 0.02
        if self.batchSize == float("inf"):
            self.batchSize = self.batchSizeInit
        else:
            latDiff = (self.minLatTarget - self.minLat) / self.minLatTarget
            print(self.batchSize)
            self.batchSize = self.batchSize + math.floor(self.batchSize * latDiff * alpha)
            if self.batchSize == float("inf"):
                self.batchSize = self.batchSizeInit
            if self.batchSize <= 1:
                self.batchSize = 2


def runSimulation(model, targetLat=5000, measInterval=100000,
                  time=5 * 60 * 1_000_000, output=None, useCoDel=True, downSize=None, upSize=None, adaptive=False, upSizeLimit=False):
    def monitor(data, resource, args):
        """Monitor queue len"""
        data.queueLenList.append(len(resource.items))
        data.logTimeList.append(resource._env.now)

    def timestamp(resource, args):
        for index in range(len(resource.items)):
            try:
                ((_, _, _), _) = resource.items[index]
                resource.items[index] = (resource.items[index], resource._env.now)
            except:
                pass

    class QueueLenMonitor:
        def __init__(self):
            self.queueLenList = []
            self.logTimeList = []

    if useCoDel:
        print('Using CoDel algorithm ...')

    env = simpy.Environment()

    # Constants
    # meanInterArrivalTime = 28500  # micro seconds
    # meanReqSize = 4096  # bytes
    # meanInterArrivalTime = 4200 # micro seconds
    # meanReqSize = 16 * 4096 # bytes
    latencyModel = StatisticLatencyModel(model)
    # OSD queue(s)
    # Add capacity parameter for max queue lengths
    osdQ1 = simpy.PriorityStore(env)
    # osdQ2 = simpy.PriorityStore(env, capacity=queueDepth)
    # osdQ = simpy.Store(env) # infinite capacity

    # KV queue (capacity translates into initial batch size)
    aioQ = VariableCapacityStore(env)
    # aioQ = simpy.Store(env, capacity=1)

    # monitoring
    queuLenMonitor = QueueLenMonitor()
    monitor = partial(monitor, queuLenMonitor)
    patchResource(osdQ1, postCallback=monitor)

    # register kvQueued Timestamp
    patchResource(aioQ, postCallback=timestamp, actions=['put'])
    patchResource(aioQ, preCallback=timestamp, actions=['get'])
    # kvQ = simpy.Store(env)

    # OSD client(s), each with a particular priority pushing request into a particular queue

    # random size and speed osd client
    # osdClientPriorityOne = RandomOSDClient(meanInterArrivalTime * 2, meanReqSize, 1, osdQ1)
    # osdClientPriorityTwo = RandomOSDClient(meanInterArrivalTime * 2, meanReqSize, 2, osdQ1)

    # 4k osd client workload generator
    osdClientPriorityOne = OsdClientBench4K(1)
    # osdClientPriorityTwo = OsdClientBench4K(2)
    ioDepth = 48
    ioDepthLockQueue = simpy.Store(env, capacity=1)
    env.process(osdClient(env, osdClientPriorityOne, osdQ1, ioDepth, ioDepthLockQueue))
    # env.process(osdClient(env, osdClientPriorityTwo, osdQ1))

    # OSD thread(s) (one per OSD queue)
    # env.process(osdThread(env, osdQ, kvQ))
    env.process(osdThread(env, osdQ1, aioQ))
    # env.process(osdThread(env, osdQ2, kvQ))

    # AIO thread in BlueStore
    # env.process(aioThread(env, aioQ, kvQ, latencyModel))

    # KV thread in BlueStore with targetMinLat and measurement interval (in usec)
    data = []
    # env.process(kvThread(env, kvQ, latencyModel, targetLat, measInterval, data))
    if adaptive:
        bm = AdaptiveBatchManagement(aioQ, targetLat, measInterval)
    else:
        bm = BatchManagement(aioQ, targetLat, measInterval, downSize=downSize, upSize=upSize, upSizeLimit=upSizeLimit)
    env.process(kvAndAioThread(env, aioQ, latencyModel, bm, ioDepthLockQueue, data=data, useCoDel=useCoDel))

    # if outputFile:
    #     env.process(outputResults(env, outputQ, outputFile))

    # Run simulation
    env.run(time)
    if output:
        with open(output, 'wb') as f:
            pickle.dump(data, f)
    duration = env.now / 1_000_000  # to sec
    bytesWritten = latencyModel.bytesWritten
    avgThroughput = bytesWritten / duration
    print(bytesWritten/4096)

    return avgThroughput, sum(queuLenMonitor.queueLenList) / len(queuLenMonitor.queueLenList), data, bm.timeLog, bm.batchSizeLog


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
    parser.add_argument('--useCoDel',
                        action='store_true',
                        help='Use CoDel algorithm for batch sizing?'
                        )
    args = parser.parse_args()
    targetLat = 250
    measInterval = 1000
    time = 5 * 60 * 1_000_000   # 5 mins

    avgThroughput, avgOsdQueueLen, data, timeLog, batchSizeLog = runSimulation(
        args.model,
        targetLat,
        measInterval,
        time,
        args.output,
        args.useCoDel
    )
    avgThroughput = avgThroughput / 1024
    print(f'Throughput: {avgThroughput} KB/s')
    print(f'OSD Queue Len: {avgOsdQueueLen}')

    # fig, ax = plt.subplots(figsize=(8, 4))
    # ax.grid(True)
    # ax.set_xlabel('time')
    # ax.set_ylabel('Batch Size')
    # ax.plot(timeLog, batchSizeLog)
    # plt.show()

