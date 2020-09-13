from abc import ABC, abstractmethod
import yaml
from scipy import stats
import math
import functools
import random


class LatencyModelConfig(object):
    def __init__(self, modelDict):
        for key in modelDict:
            if isinstance(modelDict[key], dict) and not key.startswith('_'):
                modelDict[key] = LatencyModelConfig(modelDict[key])
        self.__dict__.update(modelDict)


class LatencyModel(ABC):
    def __init__(self, configFilePath):
        self.configFilePath = configFilePath
        self.bytesWritten = 0
        self.aioBytesSubmitted = 0
        self.latencyModelConfig = None
        self.loadLatencyModelConfig()

    @abstractmethod
    def calculateKVLatency(self, size):
        pass

    @abstractmethod
    def calculateAIOLatency(self, size):
        pass

    @abstractmethod
    def calculateConstantTime(self):
        pass

    def applyWrite(self, size):
        latency = self.calculateKVLatency(size)
        latency += self.calculateConstantTime()
        self.bytesWritten += size
        return latency

    def submitAIO(self, size):
        latency = self.calculateAIOLatency(size)
        if latency > 20000:
            print(f'aio too big: {latency} >> {size}')
        self.aioBytesSubmitted += size
        return latency

    def reset(self):
        self.bytesWritten = 0

    def loadLatencyModelConfig(self):
        with open(self.configFilePath) as modelFile:
            model_dict = yaml.load(modelFile, Loader=yaml.FullLoader)
            self.latencyModelConfig = LatencyModelConfig(model_dict)


class StatisticLatencyModel(LatencyModel):
    def calculateConstantTime(self):
        lBound = self.latencyModelConfig.constantTime.lBound
        uBound = self.latencyModelConfig.constantTime.uBound
        return random.uniform(lBound, uBound)

    def calculateAIOLatency(self, size):
        if size == 0:
            return 0
        runLen = size / float(self.latencyModelConfig.writeSize)
        latency = self.calculateLatencyByDistribution(
            self.latencyModelConfig.aioWriteDistribution.distributionName,
            self.latencyModelConfig.aioWriteDistribution._params,
            1
        )
        latency *= 1_000_000  # to micro seconds
        return latency

    def calculateKVLatency(self, size):
        if size == 0:
            return 0
        runLen = size / float(self.latencyModelConfig.writeSize)
        # not affected write latency distribution
        pureWriteLatency = self.calculateLatencyByDistribution(
            self.latencyModelConfig.kvWriteDistribution.distributionName,
            self.latencyModelConfig.kvWriteDistribution._params,
            math.floor(runLen)
        )
        pureWriteLatency *= 1_000_000  # to micro seconds
        compactLat = self.calculateCompactionLatency(size)

        return pureWriteLatency + compactLat

    def calculateCompactionLatency(self, size):
        # runLen = size / float(self.latencyModelConfig.writeSize)
        compactLat = 0
        l0Frequency = self.latencyModelConfig.compaction.l0.frequency
        l1Frequency = self.latencyModelConfig.compaction.l1.frequency
        otherLevelsFrequency = self.latencyModelConfig.compaction.otherLevels.frequency
        # writesApplied = self.bytesWritten / float(self.latencyModelConfig.writeSize)
        compaction = None
        if self.bytesWritten // l0Frequency != (
                self.bytesWritten + size) // l0Frequency:
            # L0 Compaction
            compactLat += self.latencyModelConfig.compaction.l0.duration
            print('L0 Compaction')

        if self.bytesWritten // l1Frequency != (
                self.bytesWritten + size) // l1Frequency:
            # Other levels Compaction
            compactLat += self.latencyModelConfig.compaction.l1.duration
            print('L1 Compaction')

        if self.bytesWritten // otherLevelsFrequency != (
                self.bytesWritten + size) // otherLevelsFrequency:
            # Other levels Compaction
            compactLat += self.latencyModelConfig.compaction.otherLevels.duration
            print('L > 1 Compaction')
        return compactLat

    def calculateLatencyByDistribution(self, distributionName, params={}, size=1):
        distribution = getattr(stats, distributionName)
        latencies = distribution.rvs(**params, size=size)
        latencies = list(map(lambda a: abs(a), latencies))  # to micro seconds
        return functools.reduce(lambda a, b: a + b, latencies)
