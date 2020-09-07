from noisyopt import minimizeSPSA
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
        throughput, osdQueueLen, _, _, _ = radossim.runSimulation(self.model, targetLat=targetLat, measInterval=interval, time=self.time, adaptive=True, smartDownSizingSamples=1)
        print(paramList)
        return self.error(throughput, osdQueueLen)

    def error(self, throughput, osdQueueLen):
        throughputViolationPenalty = 1_000_000   # big number
        throughputChange = (self.originalThroughput - throughput) * 100 / self.originalThroughput
        if throughputChange > 10:
            return throughputViolationPenalty
        alpha = 48
        osdQLenError = alpha /osdQueueLen
        throughputError = pow(10, throughputChange - 10)
        print(f'Error for ({throughput}, {osdQueueLen}): {osdQLenError + throughputError}')
        return osdQLenError + throughputError

    def optimize(self, targetLatStartPoint, intervalBoundsStartPoint, targetLatBounds=[0, 2000], intervalBounds=[0, 10000]):
        throughput, _, _, _, _ = radossim.runSimulation(self.model, targetLat=targetLat, measInterval=interval,
                                                         time=self.time, useCoDel=False)
        self.originalThroughput = throughput
        print(f'Original Throughput: {self.originalThroughput} B/s')
        return minimizeSPSA(self.runSimulationAndCalculateError,
                            x0=[targetLatStartPoint, intervalBoundsStartPoint],
                            bounds=[targetLatBounds, intervalBounds],
                            paired=False,
                            niter=5
                            )


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
