# 历史 DWD：清洗、质量隔离与查看

本步针对完整的 2025 年 6 月官方历史记录。输入固定在已核验的 `ods.source_record` Delta 版本，原文件和 ODS 不会被本步覆盖。实现入口为 `tools/build_dwd.py`，实际 Spark 作业为 `jobs/build_dwd.py`。

## 查看入口

- `../outputs/dwd/六月数据清洗结果.html`：中文离线结果页，含各类去向、维度行数和完整每日统计。
- `../outputs/dwd/tlc_yellow-前20条.csv`、`../outputs/dwd/citibike_trips-前20条.csv`：真实加工结果中的少量明细，可用 Excel/WPS 打开。
- `../outputs/dwd/六月每日有效行程.csv`：两个方式的完整 30 天日统计，用于核对本步清洗结果。
- `../outputs/dwd/六月质量警告.csv`：保留行程中的字段警告计数，多个警告可能重叠。
- `../outputs/dwd-latest.json`：最近一次通过检查的构建，保存准确输入版本、输出版本、来源计数和规则结果。
- `../outputs/dwd-verification.json`：独立的 Parquet/ODS 追溯核验。

CSV 预览不替代完整明细表。完整表在 `data/dwd/fact_taxi_trip`、`data/dwd/fact_bike_trip`，采用 Delta 存储，应通过 Spark/Delta 日志读取；不能将目录里所有 Parquet 文件直接相加，因为重跑后可能保留已撤销版本文件。隔离原因保存在 `data/ops/quarantine_record`。

本步产生的日统计用于清洗核对，不代表正式 DWS/ADS 或 Superset 看板已发布。

## 生成内容与处理规则


纽约当地时间保留原文本和 `TIMESTAMP_NTZ`，UTC 瞬时值另存。Spark 实际引擎回归检查包括夏令时重复小时、当地不存在的时间、无法解析的文本和毫秒时长。时间解析失败/歧义进入 Q003 隔离；出租车时长不在 `(0,24小时]`、单车不在 `(0,72小时]` 进入 Q004 隔离。范围外日期保留在 ODS，并单列计数。

出租车按 ODS 记录键保留源行，内容相同但原位置不同的记录不擅自删除。距离不在 `(0,200英里]` 时保留行程，禁用距离均值资格；乘客数缺失/非正、负费用和绝对总费用超过 1000 美元有警告标志。1000 美元是本项目费用警告参考值，不是官方异常认定，也不使整条行程被剔除。

单车先检查全量 ride_id：同 ID 不同完整业务载荷会阻断该来源输出；同 ID 同内容副本按稳定 ODS 记录键选择代表。空 ride_id 进入 Q005 隔离。历史站点取该源月份真实起终点中的最常见“站名、六位小数坐标”组合，频数并列按规范化元组 JSON 的字典序选择，保留属性冲突标记。

官方边界完整 SHA-256 校验后从 EPSG:2263 转到 EPSG:4326，保留原精度，以 Shapely STRtree 做点面匹配。使用 covered_by 包含边界点；多个匹配选最小官方编码并记录警告。无坐标、不合法坐标、没有匹配区域的行程仍保留；对应空间分析应按 `origin_in_nyc`、`destination_in_nyc` 筛选并展示覆盖率。区域 GeoJSON 另行导出。

NOAA metric 值不重复缩放；核心字段质量标志非空时不可用。微量降水 T 独立分组，不能合并成无雨。天气可用性与历史行程的核心有效性分开检查。

## 计数和门禁

每个来源均核对：

`输入量 = 有效接收量 + 不同隔离原记录数 + 同内容重复跳过量 + 范围外量`

四种去向互斥。一个隔离记录可能有多个失败原因，因此隔离原因表的行数不一定等于隔离原记录数。隔离率超过配置中的 5% 时停止，不自动放宽阈值。正式输出检查逻辑键唯一及所有事实外键可解析，包括未知成员引用。

`ops.ingest_batch` 的 `logical_batch_key` 区分 ODS 与 DWD 阶段。某个来源处理完成不表示整个构建已完成；下游应只读取 `outputs/dwd-latest.json` 指定的 validated 构建与固定 Delta 版本，不自行取多张表的“最新”。

## 重跑与复现

在项目目录运行：

```powershell
.\Manage-Project.ps1 -Action Start
.\tools\Install-DwdDependencies.ps1
python tools/build_dwd.py --month 2025-06
python tools/verify_dwd.py
```

或者双击项目目录下的 `清洗六月数据.cmd`。相同输入 ODS 版本、原始对象摘要、规则、字段契约和加工代码，且输出 Delta 版本未变时，直接复用已校验构建，不新增事实版本。复用验证见 `outputs/dwd-rerun-verification.json`。需要主动重建时可加 `--force`；该选项仍是覆盖目标分区，不是追加明细。

单写入者锁在 `work/dwd/writer.lock`。原始文件变更时，先重新采集并核验 ODS，再重建 DWD；首版入口显式限制为 2025 年 6 月，扩展月份前需要同步日期/天气覆盖及发布规则。

全量批处理使用 2GB Spark driver 堆内存；作业运行期间暂时把 Spark 容器内存上限提高到 4GB，结束后恢复原配置。32 个默认 shuffle 分区降低单分区压力，并限制执行历史和代码生成缓存的保留量。事实表按日期分配写入任务，减少小文件。这些是资源和执行调整，不改变业务清洗规则。

一次中断已经完成某个来源时，可用 `--resume-run <运行ID>` 显式复用该来源。恢复入口校验 ODS 版本、原始对象、规则、字段契约、输出表版本和预览文件，构建清单保存旧/新代码摘要及来源运行ID；只适用于业务转换不变的运行恢复，不能用于绕过代码变更所需的重建。最终仍对所有来源统一检查唯一键和维度引用。JVM 突然退出后未能收尾的旧 running 批次由 `jobs/recover_interrupted_dwd_batch.py` 按该运行ID标为 failed；已完成批次保持原状态。

空间依赖的固定版本见 `config/dwd-requirements.txt`。它们安装在 D 盘挂载目录 `work/dwd/python-deps-np126`，pip 缓存在 `work/dwd/pip-cache`；容器重建后仍保留。删除整个 work 目录会删除这些运行依赖及构建清单，需要重新安装/加工；不要把它当作可随意清空的临时目录。Docker 容器内 Spark 本地中间文件也位于 D 盘的 Docker 磁盘。

## 后续工作

当前项目范围为离线数仓，见 `00-离线数仓需求与验收范围.md`。历史指标、数据质量和调度说明均继续适用。
