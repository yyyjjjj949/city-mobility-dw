# 表结构生成与校验

本目录只含模式定义，未包含业务采集与指标加工实现。正式表尚未初始化；第二步使用独立命名空间检查。

在项目根目录的 PowerShell 中：

```powershell
python .\tools\build_model.py
python .\tools\validate_model.py
```

校验要求项目Docker服务已启动，PySpark/Delta在现有Spark容器中运行；宿主Python需要能够读取时区数据库。输出见 `outputs/model-validation.json`，过程日志在 `work/model-validation`。ClickHouse校验数据库用完删除，空Delta校验目录保留在D盘供复核，正式业务目录不受影响。

`spark/001_create_lakehouse.sql` 使用容器内 `/workspace/data` 路径，它通过Compose映射到项目D盘目录。初始化时需通过现有容器执行，不要直接在Windows Spark里使用该Linux路径。

各Spark作业会话可读取并执行该DDL后使用表名；正式作业应配置自己的可追溯输入版本。首版不额外部署Hive Metastore，不能假设一次会话注册的表名会在所有新会话中自动存在。

`clickhouse/001_create_serving.sql` 仅创建空ADS表及BI视图。后续业务发布必须先写完同版本数据、核对唯一性和行数，再交换发布指针；不能先切换指针再补数据。

两份DDL重复执行不会复制数据，但不会迁移已有旧模式。结构变更需要版本化迁移。源字段逐项对应见 `config/model/source-field-mapping.json`；唯一键/外键为逻辑约束，由质量任务强制检查。
