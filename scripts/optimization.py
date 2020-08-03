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
        throughput, osdQueueLen = radossim.runSimulation(self.model, targetLat=targetLat, measInterval=interval, time=self.time)
        return self.error(throughput, osdQueueLen)

    def error(self, throughput, osdQueueLen):
        alpha = 1
        power = 1
        osdQLenError = alpha / pow(osdQueueLen, power)
        throughputViolationPenalty = 1000   # big number
        throughputChange = abs(self.originalThroughput - throughput) / self.originalThroughput
        throughputError = throughputViolationPenalty * np.sign(math.floor(10 * throughputChange))
        print(f'Error for ({throughput}, {osdQueueLen}): {osdQLenError + throughputError}')
        return osdQLenError + throughputError

    def optimize(self, targetLatStartPoint, intervalBoundsStartPoint, targetLatBounds=(500, 5000), intervalBounds=(1000, 200000)):
        throughput, _ = radossim.runSimulation(self.model, targetLat=targetLat, measInterval=interval,
                                                         time=self.time, useCoDel=False)
        self.originalThroughput = throughput
        print(f'Original Throughput: {self.originalThroughput} B/s')
        return scipy.optimize.minimize(self.runSimulationAndCalculateError, [targetLatStartPoint,
                                              intervalBoundsStartPoint],
                                      method=self.optimizationMethod,
                                      bounds=[targetLatBounds,
                                              intervalBounds])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simulate Ceph/RADOS.')
    parser.add_argument('--model',
                        metavar='filepath',
                        required=False,
                        default='latency_model.yaml',
                        help='filepath of latency model (default "latency_model_4K.yaml")'
                        )
    args = parser.parse_args()
    targetLat = 1000
    interval = 50000
    time = 5 * 60 * 1_000_000  # 5 mins
    optimizer = Optimizer('L-BFGS-B', args.model, time)
    res = optimizer.optimize(targetLat, interval)
    print(res)
