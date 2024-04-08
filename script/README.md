# Script 说明

## Local模式 && Windows环境
- 注意：所有的模型均在对应的kafka文件夹下启动，直接点击应该无效（没试过
- 在本地启动zookeeper之后，再启动kafka，如果是第一次启动kafka，需要先创建一个topic，否则会报错（参考脚本 init_new_topic.cmd）
- observe_topics.cmd 用于查看当前kafka中的topic列表
- start_zookeeper.cmd 用于启动zookeeper
- start_kafka.cmd 用于启动kafka
