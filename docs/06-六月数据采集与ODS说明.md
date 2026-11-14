# 第三步：2025年6月完整采集与ODS入仓

本步骤采集官方完整月文件，保留源文件、完整SHA-256、来源和请求期间，在Delta原始层存储逐条可追溯记录。本文记录ODS步骤；后续历史DWD、DWS/ADS、看板与指标调度已实现，详情见第07、08、10号说明。

## 数据与范围

| 来源 | 原始记录数量 | 处理方式 |
| --- | ---: | --- |
| 黄色出租车2025-06 | 4,322,960 | 完整Parquet，按row-group与组内顺序生成原始行号 |
| Citi Bike 202506 | 4,759,345 | 完整ZIP内全部5个CSV，按逻辑CSV记录计数并检查成员CRC |
| Taxi Zone编码 | 265 | 完整CSV，保留特殊区域编码 |
| NOAA中央公园日天气 | 30 | 2025-06-01—2025-06-30完整请求，metric单位并保留源属性 |
| Taxi Zone边界 | 不转换为行程记录 | 完整Shapefile ZIP，5个有效文件成员，完整CRC核验和来源登记 |


## 文件与表

- `data/ods/files/<source_id>/2025-06/`：官方原始文件及下载元数据，不改写源文件。
- `data/ods/source_record/`：Delta原始记录层，按source_id、source_period分区。
- `data/ops/ingest_batch/`：执行尝试、数量、状态和错误摘要。
- `data/ops/source_object/`：来源URL、完整SHA、大小、成员数、记录对象身份。
- `data/ops/quality_result/`：完整文件、必需字段、ODS接收计数核验结果。
- `work/ingestion/staging/`：校验过的临时Parquet分片和逐分片SHA，便于重试，不是另一套正式数仓。
- `work/ingestion/runs/<run_id>/`：每次执行的run.json、Spark日志和ods-result.json。
- `outputs/ods-ingestion-latest.json`：最近一次成功执行的来源、数量和版本结果。

所有大文件均位于D盘项目目录。数据与work目录已在Git忽略规则中，不把Citi Bike完整原始包作为独立数据集发布。

## 运行方法

先用 `启动项目.cmd` 启动环境，在项目根目录PowerShell执行：

```powershell
python .\tools\ingest_sources.py --month 2025-06
```

也可双击 `采集六月数据.cmd`。脚本需要宿主Python中的requests、pyarrow；本机已有这两个依赖。ODS写入使用已安装的Spark/Delta容器，不额外下载安装软件。

如需沿用本机已有网络代理，仅给该进程传参：

```powershell
python .\tools\ingest_sources.py --month 2025-06 --proxy http://127.0.0.1:7897
```

或在当前PowerShell设置 `CITY_MOBILITY_DOWNLOAD_PROXY`。脚本不修改Windows或Docker持久代理设置。第一次完整下载的骑行文件约929MB，断点文件保留在同一D盘目录；完整下载后才计算SHA、校验并标为可解析。

## 幂等和失败恢复

1. 文件已存在时，核对本地SHA、大小和来源元数据，不再次下载。服务器错误最多重试4次；续传需匹配ETag/Last-Modified和Range响应，源对象变化不继续拼接旧片段。
2. 已准备的Parquet分片逐个检查大小和SHA，复用可信分片。CSV含引号换行时按逻辑记录编号，不按物理文本行号。
3. 行身份使用对 `[object_id,member_path,record_ordinal]` 的确定性JSON编码SHA。相同内容的不同出租车源行保留；不同ZIP成员的同序号不是同一条源记录。
4. 先校验同一来源/月的成员数量、逐成员行号连续性与唯一性、字段和数量，再用Delta `replaceWhere` 原子替换该来源/月分区。其他来源和月份不受影响。
5. 完整源对象已经登记、ODS数量与来源对象均一致时，重复执行跳过ODS写入，记录duplicate_skipped_rows；ODS版本不增长。
6. 官方同名文件更新时，不直接混合新旧数据。下载器不会静默覆盖已跟踪文件；需核验后登记新完整对象，原始旧文件留档，再让该来源/月原子替换。旧对象在来源清单保留用于追溯。
7. Delta单表事务不能覆盖OPS多表。成功ODS写入后若来源/运行日志更新中断，重试会重新核验并完整替换同一分区，修复登记；下游仅消费validated批次及完整来源对象。
8. 运行用 `work/ingestion/ingestion.lock` 防止两个进程同时处理同一批数据。异常中断后先确认其中PID已经退出，再处理残留锁，不能盲目删除正在运行的锁。

这一步采用经验证的完整来源/月分区替换，替代第二步文档中ODS逐键MERGE的建议，以便明确移除官方修订对象的旧残留。DWD后续仍可按业务键MERGE或按受影响分区重建；不以业务内容哈希擅自删除真实记录。

## 验证边界

本次实际验证结果：首次写入9,082,600条；第二次写入0条、跳过9,082,600条；ODS Delta版本保持4。独立检查5个完整来源文件SHA、16个已提交ODS数据文件的Parquet行数和80条记录的身份/载荷SHA，全部通过。两项真实样例回归检查覆盖成员身份、同内容不同源位置、可信分片复用以及被修改分片拒绝。

验证入口：`python tools/verify_ods.py`，结果为 `outputs/ods-verification.json`。该脚本按Delta日志add/remove动作选择已提交文件，不把目录中的无效旧文件累加。

ODS检查的是完整性、必需字段、来源定位与重跑一致性，不代表业务质量规则Q003—Q018已全部执行。源时间、站点、距离、异常行程等还没有在此步骤清洗，质量报告不应将本步结果包装为业务有效率。

原始边界ZIP只登记为资产，不虚构一条JSON行程；其字节数、CRC和source_object记录是验收依据。失败任务日志保留在run目录。文件SHA是本地完整下载内容的摘要，不能声称已与未提供的官方SHA核对；完整性额外通过远端长度、响应身份和格式内部校验确认。

当前项目范围为离线数仓，见 `00-离线数仓需求与验收范围.md`。历史指标、数据质量和调度说明均继续适用。
