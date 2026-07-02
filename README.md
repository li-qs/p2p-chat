# 📡 P2P Chat

基于 Go + libp2p 的 P2P 聊天系统，无中心服务器，支持实时消息通信与文件传输。

## ✨ 功能

- 🔗 去中心化架构（无服务器）
- 🌐 mDNS 局域网自动发现
- 💬 实时文本消息通信
- 📁 文件传输（请求 → 确认 → 传输）
- 🖥 Web UI（类似微信的单页应用）
- 📡 WebSocket 实时推送（新消息、在线状态、文件状态、未读数）
- 📦 SQLite 持久化（消息、好友、传输记录）
- 🔍 slog 结构化日志，可配置日志级别

## 🚀 快速开始

```bash
go run ./cmd/app/
```

自动打开浏览器访问 Web UI。

## 📁 项目结构

```
cmd/app/          # 入口
internal/
  hub/            # 业务编排层（事件路由 + WS 推送）
    web/          # HTTP API + WebSocket + 前端模板
      tmpl/       # HTML 模板
  config/         # 配置
  domain/         # 领域模型
  infra/
    storage/      # SQLite 持久化
    transport/    # libp2p 网络层（node/session/discovery/protocol）
    event/        # 事件定义
  service/        # 已废弃，合并到 hub
pkg/utils/        # 工具函数
```

## ⚙️ 配置

```yaml
# config.yaml
multiaddrs:
  - /ip4/0.0.0.0/tcp/0
file-dir: data/files
db-path: data/db/data.db
log-level: info   # debug / info / warn / error
```

## 🔌 HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/ui` | Web UI 页面 |
| `GET` | `/ws` | WebSocket 推送 |
| `GET` | `/api/friends` | 好友列表 |
| `GET` | `/api/messages` | 历史消息 `?peerId=&lastID=` |
| `GET` | `/api/unread` | 未读计数 `?peerId=` |
| `POST` | `/api/message` | 发消息 `{"peerId":"...","text":"..."}` |
| `POST` | `/api/read` | 标记已读 `{"peerId":"..."}` |
| `POST` | `/api/file` | 发文件（multipart 或 JSON） |
| `POST` | `/api/accept-file` | 同意接收 `{"transferID":"..."}` |
| `POST` | `/api/reject-file` | 拒绝接收 `{"transferID":"..."}` |

## 📡 WebSocket 推送

| Action | 说明 | Data |
|--------|------|------|
| `0` | 刷新好友列表 | `"refresh"` |
| `1` | 新消息 | `{peerId, content, timestamp, direction, type, ...}` |
| `2` | 好友上下线 | `{peerId, online}` |
| `3` | 文件状态变更 | `{type:"file_accepted/rejected/failed/received", transferId}` |
| `4` | 未读数更新 | `{peerId, count}` |

## 🏗 架构

### Node
- 初始化 libp2p host
- 启动 mDNS 发现服务
- 注册 message / file stream handler
- 管理 peer session

### Session
- 每个 peer 一条持久 message stream（文本 + 控制信令）
- 文件传输独立 file stream（短连接）
- 自动重连、管理 stream 生命周期

### 文件传输流程
1. 发送方 → file_meta（询问是否接收）
2. 接收方 WebSocket 推送 → 前端展示 [接收] [拒绝]
3. 接收方选择 → file_accept / file_reject
4. 发送方收到 accept → 开始传输文件数据
5. 传输完成 → 双方状态更新
