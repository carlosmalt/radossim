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
def osdClient(env, priority, meanInterArrivalTime, meanReqSize, store):
        while True:
                # Wait until arrival time
                yield env.timeout(random.expovariate(1.0/meanInterArrivalTime))
                # Assemble request and timestamp
                request = (priority, random.expovariate(1.0/meanReqSize), env.now)
                # Submit request
                with store.put(request) as put:
                        yield put
                
# Batch requests and move them to BlueStore
def osdThread(env, srcQ, dstQ, batchSizeCap=None):
        while True:
                batch = []
                # Wait until there is something in the srcQ
                with srcQ.get() as get:
                        osdRequest = yield get
                        # Timestamp transaction
                        bsTxn = (osdRequest, env.now)
                        batch.append(bsTxn)
                # Determine how much to batch in addition to the req above
                if not batchSizeCap:
                        batchSize = len(srcQ.items)
                else:
                        batchSize = min(batchSizeCap-1, len(srcQ.items))
                # Do batch
                for i in range(batchSize):
                        with srcQ.get() as get:
                                osdRequest = yield get
                                # Timestamp transaction
                                bsTxn = (osdRequest, env.now)
                                batch.append(bsTxn)
                # Submit batch
                #print("batch size =", len(batch))
                with dstQ.put(tuple(batch)) as put:
                        yield put
                
# Process batch in BlueStore
def kvThread(env, store):
        latMap = {}; cntMap = {}; count = 0; lat = 0
        while True:
                # Get next batch
                with store.get() as get:
                        batch = yield get
                for bsTxn in batch:
                        # Unpack and process transaction
                        ((priority, reqSize, arrivalOSD), arrivalKV) = bsTxn
                        osdQLat = arrivalKV - arrivalOSD
                        kvQLat = env.now - arrivalKV
                        count += 1
                        lat += osdQLat + kvQLat
                        yield env.timeout(latModel(reqSize))
                        # Account latencies
                        if priority in latMap:
                                latMap[priority] += osdQLat + kvQLat
                                cntMap[priority] += 1
                        else:
                                latMap[priority] = osdQLat + kvQLat
                                cntMap[priority] = 1
                        # Periodically print latencies (averages so far)
                        if count % 10000 == 0:
                                for priority in latMap.keys():
                                        print(priority, latMap[priority] / cntMap[priority] / 1000000)
                                print('total', lat / count / 1000000)
                        #print(priority, (arrivalKV - arrivalOSD) / (env.now - arrivalKV))
                        
                        
                        
if __name__ == '__main__':
        
        env = simpy.Environment()   
        
        # Constants
        meanInterArrivalTime = 28500 # micro seconds
        meanReqSize = 4096 # bytes        
        #meanInterArrivalTime = 4200 # micro seconds
        #meanReqSize = 16 * 4096 # bytes
        
        # OSD queue(s)
        osdQ1 = simpy.PriorityStore(env) # infinite capacity
        osdQ2 = simpy.PriorityStore(env) # infinite capacity
        #osdQ = simpy.Store(env) # infinite capacity
        
        # KV queue
        kvQ = simpy.Store(env, 2) # an open batch and a committing batch
        
        # OSD client(s), each with a particular priority
        env.process(osdClient(env, 1, meanInterArrivalTime*2, meanReqSize, osdQ1))
        env.process(osdClient(env, 2, meanInterArrivalTime*2, meanReqSize, osdQ2))        
        
        # OSD thread(s) (one per OSD queue)
        # env.process(osdThread(env, osdQ, kvQ))
        env.process(osdThread(env, osdQ1, kvQ, 100))
        env.process(osdThread(env, osdQ2, kvQ, 1))
        
        # KV queue in BlueStore
        env.process(kvThread(env, kvQ))
        
        # Run simulation
        env.run(60 * 60 * 1000000)