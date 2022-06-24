# distributed_system_final_project

Spark非常重要的一个功能特性就是可以将RDD持久化在内存中。当对RDD执行持久化操作时，每个节点都会将自己操作的RDD的partition持久化到内存中，并且在之后对该RDD的反复使用中，直接使用内存缓存的partition。这样的话，对于针对一个RDD反复执行多个操作的场景，就只要对RDD计算一次即可，后面直接使用该RDD，而不需要反复计算多次该RDD。巧妙使用RDD持久化，在某些场景下，可以将Spark应用程序的性能提升数倍。基于此给出以下实验内容：

1. 在单作业任务中进行试验讨论不同的RDD持久化对于数据密集型工作的有何种程度的性能提升。
2. 在多作业任务中进行建模，通过优化的方式给出近似最优的缓存调度（该部分仅做理论推到，不涉及具体的实验内容）。


给出服务器运行的基本代码示例

```shell
# 首先需要把jar文件放入myApp内，然后需要上传数据集到input文件夹内

# 启动服务
~/hadoop-2.10.1/sbin/start-dfs.sh
~/spark-2.4.7/sbin/start-all.sh
~/spark-2.4.7/sbin/start-history-server.sh

# 上传数据集到HDFS文件系统
~/hadoop-2.10.1/bin/hdfs dfs -put ~/input/dataset.csv ./spark_input

# 删除HDFS上的输出路径
~/hadoop-2.10.1/bin/hdfs dfs -rm -r spark_output

# 提交第一部分代码（示例）
~/spark-2.4.7/bin/spark-submit --master spark://localhost:7077 --class david.final_pj.cachingWithSQL /home/ubuntu/myApp/final_pj.jar 1000000 1

# 提交第二部分代码（示例）
~/spark-2.4.7/bin/spark-submit --master spark://localhost:7077 --class david.final_pj.cachingWithSQL2 /home/ubuntu/myApp/final_pj.jar hdfs://localhost:9000/user/ubuntu/spark_input 1

```

