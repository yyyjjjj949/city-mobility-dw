# Docker Desktop 中文界面

> 2026-11-14状态更新：Windows应用控制阻止旧中文前端启动，已按SHA核验备份并恢复原厂英文前端和资源。数据库卷与业务数据保留，未修改Windows应用控制策略。下文为此前汉化记录，不代表当前仍为中文界面。当前中文文件备份在项目 `work/acceptance-2026-11-14/docker-before-restore`。

应用日期：2026-11-01。适用版本：Docker Desktop 4.97.0.244584（Windows x64）。

使用第三方项目 [asxez/DockerDesktop-CN 的 4.97.0 发布包](https://github.com/asxez/DockerDesktop-CN/releases/tag/4.97.0)。这不是 Docker 官方语言设置；少量英文提示、日志和技术名称仍保留。

下载文件：`app-Windows-x86.zip`。

SHA-256：`9047231984aeb98cc2234324db6af337ca5c0b6987926b969153416a3bf852d2`。

应用前完成本地文件审查：版本一致，原生模块内容一致，前端可执行文件仅替换 ASAR 资源头部校验值。未执行下载包自带的批处理脚本。此核对说明具体文件变化，不等同于完整安全审计。

原版备份：`D:\CodexData\workspaces\city-mobility-dw-setup\outputs\docker-language-backup\4.97.0-original`。含前端程序、界面资源、解包资源目录和逐文件 SHA-256 清单。

## 使用与恢复

从软件入口中的“02 Docker Desktop（D盘配置）”打开中文界面。

需要恢复英文时，双击项目目录的 `恢复Docker英文版.cmd`，或软件入口中的“06 恢复 Docker 英文版”。脚本先验证版本及备份校验值，再停止服务、恢复原版资源、重启 Docker 和数仓服务。当前中文解包资源会保留在 D 盘归档目录。恢复脚本已检查语法；为保留当前中文界面，未执行实际回滚。

自动更新可能覆盖汉化。此补丁及原版备份均只能用于 4.97.0；更新后须重新查找对应版本。

## 本次验证

- 已打开桌面窗口并查看截图，确认“容器、镜像、日志、卷、构建”等中文显示正常。
- 已核对安装文件与审查后的补丁文件一致。
- 六个主服务均恢复运行；五个配置了健康检查的服务均为 healthy，Spark 容器正常运行。
- Superset `/health` 和 Airflow `/api/v2/monitor/health` 均返回 HTTP 200。
- `docker-localization-verification.json` 保存服务状态及检查时间，`docker-chinese.png` 保存界面截图。

完整补丁审查记录：`D:\CodexData\workspaces\city-mobility-dw-setup\work\docker-chinese\package-audit.json`。
