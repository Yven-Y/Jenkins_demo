#!/usr/bin/python3
# -*-coding:utf-8 -*-

# ************************************************************
# @Time       : 2020/1/8 2:48 下午
# @Author     : yven
# @Project    : ota-new-demand
# @File       : ota_job.py
# @Software   : PyCharm
# @Version    : 1.0
# @Description:
#   1. 创建事物组（控制台）
#   2. web后台配置升级参数：物品组arn（可在控制台创建），开始时间，结束时间，S3_url(documentSource),targetSelection，时区
#   3.
# ************************************************************
import sys

path = ['/usr/local/bin/python3', '//miniconda3/lib/python37.zip', '//miniconda3/lib/python3.7', '//miniconda3/lib/python3.7/lib-dynload', '//miniconda3/lib/python3.7/site-packages']
for _path in path:
    sys.path.append(_path)

import boto3

import logging
import json
import datetime

import pytz

iot_client = boto3.client('iot')

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(filename)s/%(funcName)s][line:%(lineno)d] %(message)s')

logger = logging.getLogger()


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)


def my_describe_job(_job_id):
    # response = iot_client.describe_job(jobId=_job_id)
    # logger.info(json.dumps(response, cls=DateEncoder))
    # return response

    response = None
    try:
        response = iot_client.describe_job(jobId=_job_id)
        logger.info(response)
    except Exception as error:
        logger.error(str(error))
        if 'ResourceNotFoundException' in str(error):
            response = None
        else:
            response = 'error'
    return response


def my_delete_job(_job_id):
    describe_job_response = iot_client.delete_job(jobId=_job_id, force=True)
    logger.info(json.dumps(describe_job_response, cls=DateEncoder))


def my_create_job(_job_id, _targets, _document_source):
    """
    :return:
    """

    response = iot_client.create_job(
        jobId=_job_id,
        targets=_targets,
        documentSource=_document_source,
        description='yven ota job test',
        targetSelection='CONTINUOUS',

    )
    logger.info(json.dumps(response, cls=DateEncoder))
    return response


def get_time(_timezone):
    """

    :param _timezone:
    :return:
    """
    # 选择时区，生成一个时区对象
    # tz = pytz.timezone('America/New_York')
    tz = pytz.timezone(_timezone)
    # 得到指定时区的当前时间，然后将时间进行格式化
    # user_time = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    # 返回小时
    user_time = datetime.datetime.now(tz).strftime("%H")
    # print(user_time)
    return user_time


def my_describe_thing_group(_thing_group):
    response = None
    try:
        response = iot_client.describe_thing_group(thingGroupName=_thing_group)
        logger.info(response)
    except Exception as error:
        logger.error(str(error))
        if 'ResourceNotFoundException' in str(error):
            response = None
        else:
            response = 'error'
    return response


def my_create_dynamic_thing_group(_thing_group, _query):
    response = iot_client.create_dynamic_thing_group(
        thingGroupName=_thing_group,
        thingGroupProperties={
            'thingGroupDescription': 'my dynamic thing group',
            'attributePayload': {
                'attributes': {
                    'op': 'create'
                },
                'merge': True
            }
        },
        # indexName='string',
        queryString=_query,
        # queryVersion='string',
        # tags=[
        #     {
        #         'Key': 'string',
        #         'Value': 'string'
        #     },
        # ]
    )
    logger.info(response)
    return response


def my_update_dynamic_thing_group(_thing_group, _query):
    response = iot_client.update_dynamic_thing_group(thingGroupName=_thing_group,
                                                     thingGroupProperties={
                                                         'thingGroupDescription': 'my update dynamic thing group',
                                                         'attributePayload': {
                                                             'attributes': {
                                                                 'op': 'update'
                                                             },
                                                             'merge': True
                                                         }
                                                     },
                                                     queryString=_query, )
    logger.info(response)
    return response


def my_create_or_update_thing_group(_thing_group, _query_string):
    # desc
    _desc_thing_group = my_describe_thing_group(_thing_group)

    # create or update
    if not _desc_thing_group:
        my_create_dynamic_thing_group(_thing_group, _query_string)
    elif _desc_thing_group != "error":
        my_update_dynamic_thing_group(_thing_group, _query_string)
    else:
        raise Exception("An error occurred when calling the DescribeThingGroup operation")


def handler(params):
    try:
        logger.info(params)
        _timezone = params.get("timezone")

        # 其实时间点和结束时间点设置为整点小时，触发时间推荐为半小时触发一次
        _start_time = params.get("start_time")
        _end_time = params.get("end_time")

        _thing_group = params.get("thing_group")
        _query_string = params.get("query_string")

        _job_id = "%s-ota-job" % (_thing_group,)

        _document_source = params.get("document_source")

        _local_time = get_time(_timezone)

        logger.info("local_time=%s,start_time=%s,end_time=%s", _local_time, _start_time, _end_time)
        if int(_local_time) == int(_start_time):
            # 开始创建或更新job

            # 1. 获取事物组（可以在 AWS IOT 控制台进行创建，然后获取thing_group_arn）
            # 创建物品组
            my_create_or_update_thing_group(_thing_group, _query_string)
            # 获取事物组
            my_thing_group = my_describe_thing_group(_thing_group)
            thing_group_arn = my_thing_group.get('thingGroupArn', None)

            # 2.
            resp = my_describe_job(_job_id)
            if not resp:
                # 创建job
                my_create_job(_job_id, [thing_group_arn, ], _document_source)
            elif resp == 'error':
                raise Exception("An error occurred when calling the DescribeJob operation")
            else:
                logger.info("thing ota job already existed!")
        elif int(_local_time) == int(_end_time):
            # 先查询job
            resp = my_describe_job(_job_id)
            if resp:
                # 删除job
                my_delete_job(_job_id)
            else:
                logger.info("thing ota job not existed!")

        else:
            logger.info("waiting...")

    except Exception as error:
        logger.error(str(error), exc_info=True)


if __name__ == '__main__':

    logger.info("start == ")
    arg = sys.argv[1:]
    params = {
        "timezone": arg[0],
        "start_time": arg[1],
        "end_time": arg[2],
        "thing_group": arg[3],
        "query_string": arg[4],
        "document_source": arg[5],
    }
    logger.info(params)
    handler(params)
    logger.info("end == ")
