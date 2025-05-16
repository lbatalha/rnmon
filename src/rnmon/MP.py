import multiprocessing
from collections import deque

metric_queue = deque(maxlen=10000)
terminate = multiprocessing.Event()
