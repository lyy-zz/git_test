#!/usr/bin/python
# -*- coding: utf-8 -*-

# @author  : lyy
# @time    : 2021-07-30
# @function: query file attributes through the specified string
# @module : 
#   query
#       query_file_attr_sync：query file attributes synchronously, and support three query modes
#       query_file_attr_sync：query file attributes asynchronously, and support three query modes
#           iterative_query: iterative query, support multi-threading
#           query_file_attrs: query file attrs by single query str
#               parse_single_query_str: parse single query str, return filename and query attrs list
#               QueryFileAttrs: single file attrs object, use to get file attributes and print

import os
import sys
import stat
import math
import ctypes
import unittest
import argparse
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from logger import logger


class QueryFileAttrs():
    """file attrs oject
        support querying one or more file attributes
    """
    def __init__(self, file_name):
        self.file_name = file_name
        self.statinfo = os.stat(file_name)
        self.query_attr_func_dict = {
            'CreationTime': self.query_file_create_time,
            'LastAccessTime': self.query_file_last_access_time,
            'ModifiedTime': self.query_file_modified_time,
            'AllocationSize': self.query_file_allocation_size,
            'EndOfFile': self.query_file_end_of_file,
            'ReadOnly': self.query_file_read_only,
            'Hide': self.query_file_is_hide
        }
        self.query_attr_list = list(self.query_attr_func_dict.keys())

    def __timestamp2string(self, timestamp):
        str_time = ''
        try:
            dt = datetime.fromtimestamp(timestamp)
            str_time  = dt.strftime("%Y{y}%m{m}%d{d}, %H:%M:%S").format(y='年', m='月', d='日')
        except Exception as e:
            logger.warning('failed to convert {} timestamp: \
                            {} to string'.format(self.file_name, timestamp), e)
        return str_time

    def __display_attrs_query_res(self):
        print('The attributes of the file "{}"  are as follows:'.format(self.file_name))
        for attr, value in self.attrs_value_dict.items():
            print('{} = "{}"'.format(attr, value))
        print('')

    def query_file_attrs_value(self, query_attrs, display=False):
        """query file attrs value

        Args:
            query_attrs (list): query attrs list

        Returns:
            dict: a dict,example {attr1: val1, ....}
        """
        self.attrs_value_dict = {}
        if 'AllAttributes' in query_attrs:
            query_attrs = self.query_attr_list
        for query_attr in query_attrs:
            if query_attr not in self.query_attr_list:
                logger.warning('not support query {} attr'.format(query_attr))
                continue
            self.attrs_value_dict[query_attr] = self.query_attr_func_dict[query_attr]()
        
        if display:
            self.__display_attrs_query_res()
        
        return self.attrs_value_dict

    def query_file_create_time(self):
        return self.__timestamp2string(self.statinfo.st_ctime)

    def query_file_last_access_time(self):
        return self.__timestamp2string(self.statinfo.st_atime)

    def query_file_modified_time(self):
        return self.__timestamp2string(self.statinfo.st_mtime)

    def query_file_allocation_size(self):
        file_path = os.path.dirname(self.file_name)
        sectorsPerCluster = ctypes.c_ulonglong(0)
        bytesPerSector = ctypes.c_ulonglong(0)
        rootPathName = ctypes.c_wchar_p(file_path)

        ctypes.windll.kernel32.GetDiskFreeSpaceW(rootPathName,
            ctypes.pointer(sectorsPerCluster),
            ctypes.pointer(bytesPerSector),
            None,
            None,
        )
        clusterSize = sectorsPerCluster.value * bytesPerSector.value
        size_on_disk = clusterSize * math.ceil(self.statinfo.st_size / clusterSize)
        return size_on_disk

    def query_file_end_of_file(self):
        return self.statinfo.st_size

    def query_file_read_only(self):
        if (self.statinfo.st_file_attributes & stat.FILE_ATTRIBUTE_READONLY) != 0:
            return 1
        return 0

    def query_file_is_hide(self):
        if (self.statinfo.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN) != 0:
            return 1
        return 0


def _get_file_name(file_name_section):
    """get file name from file name section in query str
        check file exists, return None if not exist
    Args:
        file_name_section (str): file name section in single query str
    """
    file_name_section_split = file_name_section.strip().split('=')
    if len(file_name_section_split) < 2:
        logger.warning('file name is empty: {}'.format(file_name_section))
        return

    file_name = file_name_section_split[-1]
    if not os.path.exists(file_name):
        logger.warning('query file:{} not exist!'.format(file_name))
        return
    return file_name


def parse_single_query_str(query_str):
    """parse single query str

    Args:
        query_str (str): example "File=filename fileattr1 fileattr2"
    Returns:
        tuple: (file_name, query_attrs_list)
    """
    if len(query_str) < 1:
        logger.warning('query string is empty')
        return 

    # 切分单个查询字符串[文件名 属性1 属性2]
    query_str_split = query_str.strip().split()
    if len(query_str_split) < 2:  # 文件名+至少一个属性
        logger.warning('invalid query string format: {}'.format(query_str))
        return
    
    # 获取文件名
    file_name_section = query_str_split[0]
    if not file_name_section.startswith('File='):
        logger.warning('invalid file name format:{}, must startwith "File="'.format(file_name_section))
        return
    file_name = _get_file_name(file_name_section)
    if not file_name:
        return
    
    # 获取查询属性列表
    query_attrs_list = query_str_split[1:]

    return file_name, query_attrs_list


def query_file_attrs(query_str):
    """query file attrs by single query str

    Args:
        query_str: single query str
    Returns:
        dict: (file_name, attrs_value_dict)
    """
    parse_result = parse_single_query_str(query_str)
    if not parse_result:
        return
    file_name, query_attrs_list = parse_result
    # 初始化属性查询类
    query_obj = QueryFileAttrs(file_name)
    attrs_value_dict = query_obj.query_file_attrs_value(query_attrs_list)
    return {file_name: attrs_value_dict}


def iterative_query(iterable_query_obj, concurrent=True):
    """iterative query, support multi-threading

    Args:
        iterable_query_obj (iterable): query string list or query file object
        concurrent (bool, optional):  open multi-threaded query. Defaults to True.
    """
    query_result = {}
    # 多线程查询
    if concurrent:
        executor = ThreadPoolExecutor(os.cpu_count()*2)
        features = executor.map(query_file_attrs, iterable_query_obj)
        for feature in features:
            if not feature:
                continue
            query_result.update(feature)
    else:
        for query_str in iterable_query_obj:
            res = query_file_attrs(query_str)
            if res:
                query_result.update(res)

    return query_result


def query_file_attr_sync(query_list, concurrent=True):
    """query file attributes synchronously, and support three query modes
        query single file: `query_list` length is 1, query string startwith `File=`
        query multiple file: `query_list` length is greater than 1, each query string startwith `File=`
        file batch query: `query_list` length is 1, query string startwith `Files=`, example "Files=D:\Files.txt"
    Args:
        query_list (list): query string list
        concurrent (bool, optional): open multi-threaded query. Defaults to True.
    Returns:
        dict: {file_name1: attrs_value_dict, file_name2: ...}
    """
    if not query_list:
        logger.warning('query list is empty')
        return

    # 判断是否为多个文件查询
    if len(query_list) > 1:
        logger.debug('multi file query mode')
        return iterative_query(query_list, concurrent=True)

    query_str = query_list[0].strip() # 查询列表长度为1，取出查询字符串
    # 判断是否为文件批量查询
    if query_str.startswith('Files='):
        logger.debug('file batch query mode')
        file_name = _get_file_name(query_str)
        if not file_name:
            return

        # 遍历文件查询
        with open(file_name, encoding='utf-8') as f:
            return iterative_query(f, concurrent=True)

    # 判断是否为单个文件查询
    elif query_str.startswith('File='): 
        res = query_file_attrs(query_str)
        if res:
            return res
    else:
        logger.warning('invalid query string format: {}'.format(query_str))


def query_file_attr_async(query_list, concurrent=True):
    pass


def query(query_list, concurrent=True, block=True):
    if block:
        return query_file_attr_sync(query_list, concurrent=concurrent)
    else:
        query_file_attr_async(query_list, concurrent=concurrent)


class TestQueryFileAttrs(unittest.TestCase):

    def setUp(self):
        self.test_file_name = './test.txt'
        with open(self.test_file_name,'w') as f:
            f.write('This is test txt.')
        self.file_attr_obj = QueryFileAttrs(self.test_file_name)
        
    def test_single_query(self):
        query_str = ['File={} AllAttributes'.format(self.test_file_name)]
        query(query_str)
    
    def test_query_file_attrs(self):
        # 属性查询核心功能验证
        query_str = 'File={} AllAttributes'.format(self.test_file_name)
        query_result = query_file_attrs(query_str)
        self.assertEqual(list(query_result.keys()), self.file_attr_obj.query_attr_list)
        self.assertEqual(query_result['Hide'], 0)
        self.assertEqual(query_result['ReadOnly'], 0)
        self.assertEqual(query_result['AllocationSize'], 4096)
        self.assertEqual(query_result['EndOfFile'], 17)
        # 文件名错误
        query_str = 'File= AllAttributes'
        query_result = query_file_attrs(query_str)
        self.assertEqual(query_result, None)
        # 属性错误
        query_str = 'File={} CreationEnd'.format(self.test_file_name)
        query_result = query_file_attrs(query_str)
        self.assertEqual(query_result, {})
    
    def tearDown(self) -> None:
        os.remove(self.test_file_name)
