#!/usr/bin/env python
#-*- coding:utf-8 -*-
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

def test_query(i):
    time.sleep(1)
    # print(i)
    print('time is {}'.format(time.time()-st))

def query1(num=1):
    for i in range(num, num+5):
        test_query(i)


def query_file_attr_sync(query_list, concurrent=True):
    if concurrent:
        thread_pool = ThreadPoolExecutor(os.cpu_count()*2)
        # thread_pool.submit(query1)
        # thread_pool.submit(query1, 10)
        thread_pool.map(test_query, range(24))
    else:
        query1()
    

    

if __name__ == '__main__':
    st = time.time()
    query_file_attr_sync([], concurrent=True)
    print(os.cpu_count())

