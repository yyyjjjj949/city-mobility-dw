# Airflow完整离线调度与运行监控

更新日期：2026-11-14。操作和验收细节见 [地图、筛选与完整离线调度](13-地图筛选与完整离线调度验收.md)。

| DAG | 计划 | 工作 |
| --- | --- | --- |
| city_mobility_historical_daily | 北京时间每天02:00 | 采集/ODS → ODS验收 → DWD → DWD验收 → DWS/ADS校验发布 → 看板注册 → 图表检查 → 地图 → CSV → 健康检查 |
| city_mobility_health_monitor | 每5分钟 | 五个服务、Airflow、Superset、历史发布总量和本机执行器 |

双击 `启动项目.cmd`，等待服务启动；打开 http://localhost:18080 查看DAG和任务日志。双击 `运行完整离线流程.cmd` 可触发一次完整月度流程，也可在Airflow触发参数中填写 `{"month":"2025-06"}`。当前仅支持这个已接入月份，其他月份会在第一步失败并阻断下游。

数据与代码没有变化时会校验后复用，不重新下载源文件、不重复插入业务记录。新来源或规则变化时执行正常加工。Spark批任务占用资源时，调度步骤会等待；单DAG单运行、串行任务、失败重试一次。电脑、Docker、本机执行器必须保持运行；关机/休眠期间不会执行，catchup=False不自动积压全部历史计划。

查看 `outputs/运行状态.html` 或双击 `检查运行状态.cmd`。页面区分历史业务月份与检查时间，超过10分钟标记检查过期。`停止项目.cmd` 会等待当前执行器动作结束后停止服务并保留数据。恢复后再次启动并手动触发目标月即可复跑。

验收命令：`python tools/verify_orchestration.py` 触发真实DAG并核对全部任务；`python tools/verify_stability.py --soak-seconds 900` 持续检查服务和发布总量。报告中的实际探测时长不代表多日运行。
