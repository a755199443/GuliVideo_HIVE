# HiveETL
ETL vedio website
尚硅谷视频谷粒项目ETL之Spark版

## solution of gulivedio.txt文件预览:
数据结构:

视频表

字段		备注		详细描述

video id	视频唯一id	11位字符串

uploader	视频上传者	上传视频的用户名String

age			视频年龄	视频在平台上的整数天

category	视频类别	上传视频指定的视频分类

length		视频长度	整形数字标识的视频长度

views		观看次数	视频被浏览的次数

rate		视频评分	满分5分

Ratings		流量		视频的流量，整型数字

conments	评论数		一个视频的整数评论数

related ids	相关视频id	相关视频的id，最多20个

用户表

字段		备注			字段类型

uploader	上传者用户名	string

videos		上传视频数		int

friends		朋友数量		int

数据准备:

linux运行spark程序

bin/spark-submit --master yarn --class com.hive.ETLUtils /root/hiveETL.jar file:///root/in/video/2008/0222 /data/input/video/2008/0222/ 

hive初始优化

//对于小数据,启动本地化模式可以有效缩短任务时间

set hive.exec.mode.local.auto=true;

//设置local mr的最大输入数据量，当输入数据量小于这个值时采用local  mr的方式，默认为134217728，即128M

set hive.exec.mode.local.auto.inputbytes.max=500000000;

//设置local mr的最大输入文件个数，当输入文件个数小于这个值时采用local mr的方式，默认为4

set hive.exec.mode.local.auto.input.files.max=10;
