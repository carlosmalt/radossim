import simpy
from functools import partial, wraps


class VariableCapacityStore(simpy.resources.store.Store):
    def __init__(self, env, capacity=float('inf')):
        super(VariableCapacityStore, self).__init__(env, capacity)

    def changeCapacity(self, capacity=float('inf')):
        self._capacity = capacity


class SecondaryCapacityStore(simpy.resources.store.Store):
    def __init__(self, env, capacity=float('inf')):
        super(SecondaryCapacityStore, self).__init__(env, capacity)
        self._secondaryCapacity = capacity

    def _do_put(self, event):
        if len(self.items) < self._secondaryCapacity:
            return super(SecondaryCapacityStore, self)._do_put(event)
        return None

    def changeCapacity(self, capacity=float('inf')):
        self._secondaryCapacity = capacity


def patchResource(resource, preCallback=None, postCallback=None, actions=None):
    """Patch *resource* so that it calls the callable *preCallback* before each
    put/get/request/release operation and the callable *postCallback* after each
    operation.  The only argument to these functions is the resource
    instance.
    """

    if actions is None:
        actions = ['put', 'get']

    def getWrapper(func):
        # Generate a wrapper for put/get/request/release
        @wraps(func)
        def wrapper(*args, **kwargs):
            # This is the actual wrapper
            # Call "pre" callback
            if preCallback:
                result = preCallback(resource, args)
                if result:
                    args = result

            # Perform actual operation
            ret = func(*args, **kwargs)

            # Call "post" callback
            if postCallback:
                postCallback(resource, args)

            return ret

        return wrapper

    # Replace the original operations with our wrapper
    for name in actions:
        if hasattr(resource, name):
            setattr(resource, name, getWrapper(getattr(resource, name)))


if __name__ == "__main__":
    def test1(env, queue):
        i = 2
        while True:
            with queue.put(i) as put:
                yield put
                # print(f'put {i}')
            i += 1

    def test2(env, queue):
        queue.changeCapacity(1)
        while True:
            batch = []
            with queue.get() as get:
                req = yield get
                queue.changeCapacity(req)
                batch.append(req)
            for i in range(len(queue.items)):
                with queue.get() as get:
                    req = yield get
                    batch.append(req)
            print(len(batch))
            yield env.timeout(100)


    env = simpy.Environment()
    # aioQ = VariableCapacityStore(env, capacity=1000)
    aioQ = SecondaryCapacityStore(env, capacity=1000)
    env.process(test1(env, aioQ))
    env.process(test2(env, aioQ))
    env.run()
