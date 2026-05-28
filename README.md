# 📡 P2P Chat

一个基于 Go + libp2p 实现的轻量级 P2P 聊天系统，无中心服务器，支持实时消息通信与文件传输。

---

## ✨ 功能特性

- 🔗 去中心化架构（无服务器）
- 🌐 mDNS 自动局域网发现
- 💬 基于 libp2p stream 的实时消息通信
- 📁 独立 file stream 文件传输
- 🧩 可扩展的消息协议结构，支持作为通用信令通道使用
- 🚧 当前仅支持 CLI 模式，图形化 UI 正在开发中

---

## 🧱 系统架构

### 核心模块

#### Node

- 初始化 libp2p host
- 启动 mDNS 发现服务
- 注册 message / file stream handler
- 管理 SessionManager

#### Session

- 表示一个 peer 的通信上下文
- 管理 message stream 生命周期
- 记录 lastActive 活跃时间
- 负责发送/接收调度

#### Message Stream（长连接）

- 每个 peer 一条持久 stream
- 用于：
  - 文本消息
  - 控制消息（FileMeta / AcceptFile / RejectFile）

#### File Stream（短连接）

- 每个文件传输独立 stream
- 生命周期短，一次性使用
- 与 message stream 完全隔离

## ToDo list

- [ ] GUI
- [ ] peer 信息
- [ ] 历史消息
- [ ] 文件记录
