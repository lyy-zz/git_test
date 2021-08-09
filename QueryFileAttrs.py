#!/usr/bin/python
# -*- coding: utf-8 -*-

# @author  : lyy
# @time    : 2021-08-10
# @function: query file attributes through the specified string
# @module : 
#   traverse_query_str_list: traverse query str list(input), support file query mode
#   query_file_attrs: query file attrs by single query str
#       parse_single_query_str: parse single query str, return filename and query attrs list
#       QueryFileAttrs: single file attrs object, use to get file attributes and print
import sys
import argparse

from logger import logger
from query import query

def display_attrs_query_res(query_result):
    for file_name, attrs_value_dict in query_result.items():
        print('The attributes of the file "{}"  are as follows:'.format(file_name))
        for attr, value in attrs_value_dict.items():
            print('{} = "{}"'.format(attr, value))
        print('')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='query attributes of files')
    parser.add_argument('query_str',type=str, nargs='+',
                        help='query string, include file name and attr')
    


    if not sys.platform.startswith('win'):
        logger.error('Only supports window platform!')
        sys.exit()
    
    args = parser.parse_args()

    block = False
    if block:
        query_result = query(args.query_str, concurrent=True, block=True)
        if query_result:
            display_attrs_query_res(query_result)

    # 异步调用，需传入回调函数用于处理查询结果
    query(args.query_str, concurrent=True, block=False, callback=display_attrs_query_res)
