import scipy.optimize
import radossim
import math
import numpy as np
import argparse


class Optimizer:
    def __init__(self, optimizationMethod, model, time):
        self.originalThroughput = 0
        self.time = time
        self.model = model
        self.optimizationMethod = optimizationMethod

    def runSimulationAndCalculateError(self, paramList):
        targetLat, interval = paramList
        throughput, osdQueueLen, data, _, _ = radossim.runSimulation(self.model, targetLat=targetLat, measInterval=interval, time=self.time, adaptive=True, smartDownSizingSamples=1)
        print(paramList)
        return self.error(throughput, osdQueueLen, data)

    def error(self, throughput, osdQueueLen, data):
        throughputViolationPenalty = 1_000_000   # big number
        throughputChange = (self.originalThroughput - throughput) * 100 / self.originalThroughput
        if throughputChange > 10:
            return throughputViolationPenalty
        avgKVQueueLat = 0
        for (((((_, _, _), _), arrivalKV), _, _), kvQDispatch, kvCommit) in data:
            avgKVQueueLat += kvQDispatch - arrivalKV
        avgKVQueueLat /= len(data)
        print(f'Error for ({throughput}, {throughputChange}, {avgKVQueueLat})')
        return avgKVQueueLat

    def optimize(self, targetLatStartPoint, intervalBoundsStartPoint, targetLatBounds=(10, 2000), intervalBounds=(10, 10000)):
        throughput, _, _, _, _ = radossim.runSimulation(self.model, targetLat=targetLat, measInterval=interval,
                                                         time=self.time, useCoDel=False)
        self.originalThroughput = throughput
        print(f'Original Throughput: {self.originalThroughput} B/s')
        return scipy.optimize.minimize(self.runSimulationAndCalculateError, [targetLatStartPoint,
                                              intervalBoundsStartPoint],
                                      method=self.optimizationMethod,
                                      bounds=[targetLatBounds,
                                              intervalBounds],
                                       options={'eps':200})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simulate Ceph/RADOS.')
    parser.add_argument('--model',
                        metavar='filepath',
                        required=False,
                        default='latency_model.yaml',
                        help='filepath of latency model (default "latency_model_4K.yaml")'
                        )
    args = parser.parse_args()
    targetLat = 250
    interval = 1000
    time = 10 * 1_000_000
    optimizer = Optimizer('L-BFGS-B', args.model, time)
    res = optimizer.optimize(targetLat, interval)
    print(res)
