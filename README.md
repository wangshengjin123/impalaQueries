# impalaQueries   
#项目是基于https://github.com/rmarshasatx/ImpalaQueries  对脚本2无法执行 做了一些修改适应当前cdh5.16.1环境


通过命令抓取一段时间impala执行的历史sql，输出到bb.json
python3 collect_impala_queries.py 2020-11-06-8 2020-11-09-23 bb.json

通过第二个脚本解析——多个字段  -f=file    -m=过滤超过这个时间的sql   -q=队列     ————可以用了过滤慢sql
python3 impala-sql1.py -f /root/wang/ImpalaQueries/bb.json -m 100 -q default >>sql100.sql

通过第三个脚本解析——一个字段
python3 impala-sql2.py -f /root/wang/ImpalaQueries/bb.json -m 1000 -q default >>sql1000.sql

对sql文件进行一些人工的处理，去掉一些全量查询  查询报错的sql
对impala执行并发测试,并计算开始结束时间
sh impala-test.sh 10 sql1000.sql && date
