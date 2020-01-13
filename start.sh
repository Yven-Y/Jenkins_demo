#!/bin/bash

# aws account
region_name="ap-south-1"
aws_access_key_id="******"
aws_secret_access_key="******"

# 时区
timezone="America/Montreal"
# 其实时间点和结束时间点设置为整点小时，触发时间推荐为半小时触发一次
start_time="01"
end_time="02"

# 物品组信息，会创建或者更新
thing_group="******"
# 物品组筛筛选条件
query_string="******"

# OTA升级文件 s3地址
document_source="******"

# 执行Python脚本
python3 ota_job_integration_main.py $region_name $aws_access_key_id $aws_secret_access_key $timezone $start_time $end_time $thing_group $query_string $document_source