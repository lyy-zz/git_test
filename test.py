#!/usr/bin/env python
#-*- coding:utf-8 -*-
from math import remainder
import os
import threading
import time
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed

global st, et


def log(func):
    def wrapper(*args, **kwargs):
        def log_task(user):
            time.sleep(10)
            print (user, "login log")

        threading.Thread(target=log_task, args=("user 1",)).start()
        return func(*args, **kwargs)
    return wrapper

def mock_query(i):
    time.sleep(1)
    print('time is {}'.format(time.time()-st))
    return i


def query_file_attr_sync(num, concurrent=True):
    query_result = []
    if concurrent:
        executor = ThreadPoolExecutor(os.cpu_count()*2)
        features = executor.map(mock_query, range(num))
        for feature in features:
            query_result.append(feature)
    else:
        for i in range(num):
            res = mock_query(i)
            query_result.append(res)

    return query_result
    

def query_file_attr_async(num, concurrent=True):
    threading.Thread(target=query_file_attr_sync, args=(num, concurrent)).start()
    pass
    

if __name__ == '__main__':
    st = time.time()
    # res = query_file_attr_sync(24, concurrent=False)
    query_file_attr_async(24, concurrent=True)
    print("+++++++++++  end  +++++++++++\n")
    time.sleep(2)
    print(res)
