# 城市出行离线数仓组员运行包

## 两种包

- `城市出行数仓-成果展示包.zip`：不需要安装 Docker，双击 `打开展示.html` 即可看地图、筛选、清洗结果、CSV 和看板截图。
- `CityMobility`：完整运行包，包含真实数据、五个 Docker 镜像、数据库备份、Docker安装程序和内置 Python。建议目标磁盘至少预留 **50GB**，用于压缩包、解压文件、Docker镜像、数据卷和后续加工临时文件。

## 完整包安装

1. Windows 10/11 64 位电脑安装并启动 Docker Desktop（安装程序在 `installers`），确认使用 **Linux containers**，建议内存 16GB 以上。Docker使用WSL2；第一次安装可能需要按提示启用虚拟化、更新WSL或重启。业务文件已打包，项目导入和演示无需重新下载源数据或镜像。
2. 将 `CityMobility` 解压到短路径，例如 `D:\CityMobility`。不要放在 OneDrive、微信同步目录或带很长中文路径的目录。
3. 双击 `01-first-install.cmd`。首次会导入五个 Docker 镜像、恢复数据库和看板，可能需要几分钟。
4. 安装成功后，双击 `02-start.cmd`。浏览器打开：
   - Superset：`http://localhost:8088`
   - Airflow：`http://localhost:18080`
   - 地图：`http://localhost:8088/city-mobility/map/`
5. 账号：`admin`，密码：`YOUR_LOCAL_PASSWORD`。

日常操作：`04-check.cmd` 检查服务，`05-run-full-pipeline.cmd` 触发已验证的 2025 年 6 月离线流程，`03-stop.cmd` 停止服务，`06-open-dashboard.cmd` 打开总览看板。

项目静态入口是 `outputs/项目总入口.html`，地图、CSV与设计图可以直接打开。运行状态页面显示最近一次检查的时间，完整服务启动后Airflow会继续定时更新。

首次安装脚本具有保护措施：发现同名数据库卷时会拒绝覆盖，不会删除组员已有项目。若端口被占用，可在 `.env` 修改 `MOBILITY_SUPERSET_PORT`、`MOBILITY_AIRFLOW_PORT`、`MOBILITY_CLICKHOUSE_PORT` 后重新执行启动和检查。

首次完整安装只执行一次，之后直接使用 `02-start.cmd`。安装报错时保留错误文字和目录，不要随意删除数据库卷；安装入口会拒绝覆盖未识别的数据库。根目录 `package-manifest.json` 保存逐文件SHA-256，可运行 `runtime\python\python.exe tools\portable.py verify-files` 检查尚未运行过的原始解压包完整性。

镜像包含Linux运行所需软件，不必另外安装Spark、Java、PostgreSQL、ClickHouse、Superset、Airflow、Anaconda、DBeaver或Python。包内已有Windows Python 3.13.9与项目依赖。架构图可直接查看PNG，编辑原图时再自行安装draw.io。

运行中的项目使用独立Docker项目名 `city-mobility-team`。默认端口为8088、18080、18123、9000及4040。更改端口时，启动脚本会显示新的看板和调度地址；历史静态页面中的默认localhost链接可能仍需手动改为新端口。

本包当前只支持 `month=2025-06`。完整到达量和净流入因为缺少五月边界数据保持不可用。运行包用于课程演示，不要把 `.env`、数据库密码或 `backups` 上传到公开仓库。

## 本次交付验证

已在打包电脑上，使用独立Docker项目名 `city-mobility-transfer-check` 和独立端口，实际导入镜像、恢复数据库并运行完整十步调度。9012795条有效行程、4个看板、19张图表、日期和出行方式筛选均通过检查。详情见 `outputs/migration-verification.json`。这属于同一台电脑上的隔离迁移测试，尚未在其他组员的实体电脑上实测。

`outputs` 中早期验收报告保留了原项目的测试时间与源路径，表示历史证据，不代表组员电脑的当前运行状态；每次安装或检查会生成该电脑自己的 `member-verification.json`。
