# Simulate the interaction between osd queues and kv queue that causes
# bufferbloat
# use dispatching mechanism

import simpy
import random
import math

# global variable for state monitoring
enableBlocking = True
blockNext = 0
totalBytes = 0
totalRuntime = 0
avgThroughput = 0
averageLatency = 0

# codel variables
codel_const_lat = 300
codel_target_lat = 1450

# collecting internal states
blocking_dur_vec = [] # store all generated blocking duration
osd_queue_size_vec = []   # snapshot osd_queue size
kv_queue_size_vec = []    # snapshot kv_queue size
bs_lat_vec = []      # store (priority,bs_lat = kv_queueing_lat + commit_lat,osdArrTime,kvArrTime)
total_lat_vec = [] # osd_lat + bs_lat
min_lat_vec = []      # store all min_lat

# Predict request process time in micro seconds (roughly based on spinning media model
# kaldewey:rtas08, Fig 2)
def latModel(
    reqSize, lgMult=820.28, lgAdd=-1114.3, smMult=62.36, smAdd=5905.97, mu=5.1, sigma=.415):
    # Request size-dependent component
    runLen = reqSize / 4096.0
    if runLen > 16:
        iops = lgMult * math.log(runLen) + lgAdd
    else:
        iops = smMult * runLen + smAdd
    sizeLat = int((1000000 / iops) * runLen)
    # Latency component due to compaction (see skourtis:inflow13, Fig 4)
    compactLat = random.lognormvariate(mu, sigma)
    return sizeLat + compactLat


# Create requests with a fixed priority and certain inter-arrival and size distributions
def osdClient(env, priority, meanInterArrivalTime, meanReqSize, dstQ):
    while True:
        # Wait until arrival timev(normal distribution)
        #yield env.timeout(random.expovariate(1.0 / meanInterArrivalTime))
        #uniform distribution
        yield env.timeout(meanInterArrivalTime)
        # Assemble request and timestamp(normal distribution)
        #request = (priority, random.expovariate(1.0 / meanReqSize), env.now)
        # uniform distribution
        request = (priority, meanReqSize, env.now)
        # Submit request
        with dstQ.put(request) as put:
            yield put            


# Move requests to BlueStore
def osdThread(env, srcQ, dstQ):
    #global totalBytes
    while True:
        # Wait until there is something in the srcQ
        with srcQ.get() as get:
             
            osdRequest = yield get
            #totalBytes += osdRequest[1]
            # Timestamp transaction (after this time request cannot be prioritized)
            curTime = env.now
            if curTime < blockNext:
                yield env.timeout(blockNext-curTime) # this is wait_until
            bsTxn = (osdRequest, env.now)
        osd_queue_size_vec.append(len(srcQ.items))
        # Submit BlueStore transaction
        with dstQ.put(bsTxn) as put:
            yield put

            
# Batch incoming requests and process
def kvThread(env, srcQ, targetLat=5000, measInterval=100000):
    global totalBytes
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
        kv_queue_size_vec.append(batchSize)
        kvQDispatch = env.now
        yield env.timeout(latModel(batchReqSize))
        kvCommit = env.now - kvQDispatch
        # Diagnose and manage batching
        bm.manageBatch(batch, batchReqSize, kvQDispatch, kvCommit)
        totalBytes += batchReqSize 
        #print(batchReqSize,totalBytes)

    
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
        self.preBlockingDur = codel_const_lat
        self.curBlockingDur = 0
        self.constLat = codel_const_lat
        # Batch sizing state
        self.batchSize = self.queue.capacity
        self.batchSizeInit = 100
        self.batchDownSize = lambda x: int(x / 2)
        self.batchUpSize = lambda x: int(x + 10)

    def manageBatch(self, batch, batchSize, dispatchTime, commitTime):
        systemNow = env.now
        self.minLat = 0
        for txn in batch:
            ((priority, reqSize, arrivalOSD), arrivalKV) = txn
            # Account latencies
            osdQLat = arrivalKV - arrivalOSD # op_queue latency
            kvQLat = dispatchTime - arrivalKV # kv_queue latency
            bs_lat = kvQLat + commitTime # kv_queue queueing time plus commit time
            bs_lat_vec.append((priority,bs_lat,txn[0][2],txn[1]))
            total_lat_vec.append((priority,osdQLat + bs_lat,txn[0][2],txn[1]))
            #print((priority,bs_lat,txn[0][2],txn[1]))
            #if not self.minLat or kvQLat < self.minLat:
            if not self.minLat or bs_lat < self.minLat:
                self.minLat = bs_lat # get the min lat in a batch
            self.count += 1
            #self.lat += osdQLat + kvQLat # total latency
            self.lat += osdQLat + bs_lat # total latency
            if priority in self.latMap:
                #self.latMap[priority] += osdQLat + kvQLat
                self.latMap[priority] += osdQLat + bs_lat
                self.cntMap[priority] += 1
            else:
                #self.latMap[priority] = osdQLat + kvQLat
                self.latMap[priority] = osdQLat + bs_lat
                self.cntMap[priority] = 1
            #self.fightBufferbloat(kvQLat, dispatchTime)
            #self.printLats()
        # enable compareLatency function to fight buffer bloat
        if enableBlocking is True:
            self.compareLatency(systemNow)

    # CoDel regulate the number of txns dispatched to BlueStore
    def compareLatency(self, currentTime):
        global blockNext
        min_lat_vec.append(self.minLat)
        # rule 01 ------------->
        if self.minLat <= self.minLatTarget:
            self.curBlockingDur = self.preBlockingDur / 2
        else:
            self.curBlockingDur = self.preBlockingDur + self.constLat
        # rule 01 <-------------
        blocking_dur_vec.append(self.curBlockingDur)
        self.preBlockingDur = self.curBlockingDur
        blockNext = currentTime + self.curBlockingDur
        
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
    env = simpy.Environment()
    print("enableBlocking =",enableBlocking)
    # Constants
    meanInterArrivalTime = 200 #28500  # micro seconds
    meanReqSize = 4096  # bytes
    # meanInterArrivalTime = 4200 # micro seconds
    # meanReqSize = 16 * 4096 # bytes

    # OSD queue(s)
    # Add capacity parameter for max queue lengths
    osdQ1 = simpy.PriorityStore(env)
    #osdQ2 = simpy.PriorityStore(env)
    # osdQ = simpy.Store(env) # infinite capacity

    # KV queue (capacity translates into initial batch size)
    #kvQ = simpy.Store(env, 1)
    kvQ = simpy.Store(env, 20)

    # OSD client(s), each with a particular priority pushing request into a particular queue
    env.process(osdClient(env, 1, meanInterArrivalTime * 2, meanReqSize, osdQ1))
    env.process(osdClient(env, 2, meanInterArrivalTime * 2, meanReqSize, osdQ1))

    # OSD thread(s) (one per OSD queue)
    # env.process(osdThread(env, osdQ, kvQ))
    env.process(osdThread(env, osdQ1, kvQ))
    #env.process(osdThread(env, osdQ2, kvQ))
    #print(len(kvQ.items))

    # KV thread in BlueStore with targetMinLat and measurement interval (in usec)
    #env.process(kvThread(env, kvQ, 80000, 1600000))
    # codel: change $3 to adjust target_delay 
    env.process(kvThread(env, kvQ, codel_target_lat, 1600000))

    # Run simulation
    #env.run(120 * 60 * 1000000) # 2 hrs
    totalRuntime = 1 * 60 * 1000000 # 1 mins
    env.run(until=totalRuntime)
    
    # print CoDel parameters
    print("-------- CoDel Parameters --------")
    print("target lat =",codel_target_lat)
    print("const lat =",codel_const_lat)
    
    # print average throughput
    print("----------------------------------")
    avgThroughput = totalBytes / (totalRuntime / 1000000)
    print("total bytes =",totalBytes)
    print("total time(s) =",totalRuntime / 1000000)
    print("average throughput(MB/s) =",avgThroughput / 1048576)