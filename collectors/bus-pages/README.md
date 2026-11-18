# 独立业务页面采集器

用途：逐页解析Bus Times上Cambridge地点列表的线路详情和计划时刻，输出可追溯的本地HTML及结构化JSON。与旧NYC文件获取流程分开，不把目录发现包装成业务记录采集。

依赖：Node.js 24.5及以上（本机24.16.0），Cheerio 1.2.0、robots-parser 3.0.1，锁定版本见package-lock.json。npm缓存按本机设置留在D盘。

在本目录执行：

~~~powershell
npm ci --ignore-scripts
node --test
node --use-env-proxy crawl.mjs --max-pages 100
~~~

采集默认最多20页；本轮实际用100的上限，列表发现62页，全部访问。固定种子及站点白名单，不需要账号、代理轮换或浏览器模拟。顺序访问间隔至少2.2秒，并遵守更长的robots crawl-delay；不访问日期查询、API、文件下载、线路子页。403等情况记录失败，不绕过；429停止本轮。

输出默认为本项目work/2026-11-18-theme-definition-e721eb90/business-page-crawl/<run-id>的原始HTML/JSON，以及outputs下同名独立目录的公开安全回执。可通过--work-root和--output-root指定本次D盘目录，不覆盖既有轮次。HTML只留本地，勿上传node_modules、整站HTML或完整逐条数据。

Python清洗器位于tools/clean_bus_page_snapshot.py，显式读取本地--input-run，不进行网络采集。具体日期、固定周类型日历和未来计划分别保留；跨行时刻不复制为多条事实。

修订解析器后可对本地原快照重解析，reparse-local.mjs不联网（从本目录执行）：

~~~powershell
node reparse-local.mjs 20261118T114451Z-38d2cc
~~~

完整范围、示例及新旧业务关系见docs/22-教师二次反馈与业务网页采集试验.md。当前只验证了新来源与清洗，还未切换正式数仓。
