# Docker安装登记恢复与验收

更新时间：2026年11月18日（北京时间）。本文件补充项目报告初次复核时发现的Docker启动问题，说明本轮已经执行的修复与恢复验收。

## 1. 原因与修复

Docker Desktop 4.97.0程序文件位于D盘，官方数字签名有效。其后端在启动时读取Windows安装登记失败，日志重复报告找不到 `HKLM\SOFTWARE\Docker Inc.\Docker Desktop`。64位和32位HKLM/HKCU检查均未找到该安装项。登记丢失的具体历史原因尚不能确定，不能归因于某个清理软件或用户操作。

程序目录、WSL发行版及原数据盘均存在。项目实际使用的配置为 `D:\CodexData\app-data\docker-profile`，原数据盘为 `D:\CodexData\data\docker-desktop\disk\docker_data.vhdx`，大小20,843,593,728字节（约19.4GiB）。

2026年11月18日18:33:27，在用户通过正常Windows UAC授予管理员权限后，修复脚本验证已有程序版本、Docker官方签名和必需文件，仅恢复上述64位HKLM项中的AppPath字符串，指向已有D盘安装目录。没有覆盖其他安装路径，没有卸载、重装、恢复出厂设置或清空数据，也没有更改账号密码。随后通过项目原启动入口使用D盘专用配置启动Docker及五个服务。

## 2. 现场服务验收

18:34:55—18:35:05完成12项检查，结果见[服务恢复验收](../outputs/2026-11-18-theme-definition-e721eb90/verification/docker-environment-repair.json)：

| 检查对象 | 实际结果 |
| --- | --- |
| 安装登记与数据位置 | AppPath正确；继续使用原D盘配置和原VHDX |
| Docker引擎 | Linux引擎29.9.2可用，5个容器运行 |
| 原已登记镜像 | 5个已登记非Kafka基础镜像身份与历史清单一致 |
| 项目服务 | PostgreSQL、ClickHouse、Airflow、Superset为running/healthy；Spark为running，其Compose定义没有额外健康探针 |
| 已发布行程 | 出租车4,254,337条，骑行4,758,458条，合计9,012,795条 |
| 发布指针 | historical和quality仍指向fa3ddf3a864140a7b6893836d3215a39 |
| BI | Superset健康接口、账号登录成功；4个看板、19张图表保留 |
| 调度 | Airflow数据库、scheduler、triggerer、dag_processor健康；本机执行器心跳正常 |

本轮不是只读取历史JSON来证明服务恢复，而是通过Docker命令、ClickHouse实际查询、Superset认证API和Airflow健康接口现场检查。

## 3. 十步调度恢复

Docker恢复后，Airflow自动处理了此前排队的计划实例。实际执行时间为2026年11月18日18:34:15—18:36:48，十个任务均成功：采集/ODS入仓验收、ODS核查、DWD构建复用、DWD核查、指标构建复用、看板注册、图表核查、地图导出、预览导出、项目检查。

该实例ID为 `scheduled__2026-11-14T18:00:00+00:00`，其中日期是计划标识；全部十项任务的本轮实际起止时间均为11月18日，详见[调度恢复回执](../outputs/2026-11-18-theme-definition-e721eb90/verification/docker-pipeline-recovery.json)。采集任务成功尝试号为2，其余为1；未把重试改写为首次成功。

DWD和指标采用已有已验证构建的幂等复用。本轮没有强制重新全量计算，也没有另外人工触发一个重复实例。此次成功证明恢复后该实例完整执行，不等于重新完成多日稳定性、断电恢复或所有故障注入验收。

## 4. Kafka提示

Compose提示存在`city-mobility-dw-kafka-1`和`city-mobility-dw-kafka-init-1`两个orphan containers。它们来自已停用的实时模块，本轮检查均为exited，`restart=no`，不占用当前五个离线服务的运行名额。该提示不表示启动失败。本轮保留这些旧容器，没有执行`--remove-orphans`、`volume prune`或其他删除操作。

## 5. 当前入口与边界

- Superset：本机8088端口。
- Airflow：本机18080端口。
- 后续正常启动继续使用项目根目录的“启动项目.cmd”；本轮注册修复已经完成，无需每次重复执行管理员修复。
- 账号密码沿用原配置，不在公开仓库写入实际凭据。
- 数据仅验收2025-06；完整到达量和净流入等原有限制继续适用。

综合状态见[本轮整改验证](../outputs/2026-11-18-theme-definition-e721eb90/verification/整改验证.json)。现场状态具有时间范围，未来关机或停止服务后应再次检查。
