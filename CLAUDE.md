# 文档创建规范（docs/）

本仓库的过程文档统一存放在 `docs/` 目录，并遵循以下**命名**规范。

## 文件命名（必须）

格式：

`<type>_<YYYY-MM-DD>_<title>.md`

- `type` 取值：
  - `fix`：针对某个 bug 的修复方案/复盘/实施记录
  - `analysis`：对现象、问题、系统的分析/调研/报告
  - `feat`：功能/模块的设计文档（不属于 fix/analysis 的默认归类）
- `YYYY-MM-DD`：文档**创建日期**（精确到天）；后续更新不改日期
- `title`：与文档首行 `# ...` 的标题一致（建议简洁明确，使用中文）

示例：

- `fix_2026-02-27_ElevenLabs TTS WebSocket 断连不重连问题修复.md`
- `analysis_2026-02-12_打断输入逻辑分析报告.md`
- `feat_2026-02-13_SmallWebRTC Transport 集成设计文档.md`
