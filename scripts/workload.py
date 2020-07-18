from abc import ABC, abstractmethod
import random


class WorkloadGenerator(ABC):
    @abstractmethod
    def calculateTimeout(self):
        return 0

    @abstractmethod
    def createRequest(self, env):
        return ()


class OsdClientBench4K(WorkloadGenerator):
    def __init__(self, requestSize, priority):
        self.requestSize = requestSize
        self.priority = priority

    def calculateTimeout(self):
        return 0

    def createRequest(self, env):
        return self.priority, self.requestSize, env.now


class RandomOSDClient(WorkloadGenerator):
    def __init__(self, meanInterArrivalTime, meanReqSize, priority):
        self.meanReqSize = meanReqSize
        self.meanInterArrivalTime = meanInterArrivalTime
        self.priority = priority

    def calculateTimeout(self):
        return random.expovariate(1.0 / self.meanInterArrivalTime)

    def createRequest(self, env):
        return self.priority, random.expovariate(1.0 /self. meanReqSize), env.now

