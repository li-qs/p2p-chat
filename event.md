# 事件处理

## 来自通讯内核的通知：

### transport.PeerConnectedEvent - 对端建立连接

此事件表示对端已经上线并建立连接，程序内部已建立 session，应该保存对端信息，并刷新 UI 中的好友列表。

### transport.PeerDisconnectedEvent - 对端断开连接

此事件表示对端已经下线，程序内部已清理掉 session，应该标记为离线，并刷新 UI 中的好友列表，但仍可以查看历史聊天记录。

### transport.FileTimeoutEvent - 文件传输请求超时

此事件表示对端一直没有答复文件传输请求，程序内部已关闭传输，应该标记为超时或关闭，并在 UI 中提醒和更新文件状态。

### transport.FileReceivedEvent - 文件接收完毕

此事件表示文件已经接收完毕，程序内部已将文件保存到指定文件夹，应该标记为成功或结束，并在 UI 中提醒和更新文件状态。

## 来自对端的通知：

### transport.MessageEvent - 收到文本消息

此事件表示收到对端发送的文本信息，应该保存到历史聊天消息中，并在 UI 中发送新消息提醒、刷新聊天记录。

### transport.FileMetaEvent - 收到文件元数据/收到文件传输请求

此事件表示对端请求发送一个文件，应该保存到历史聊天消息中、保存文件传输信息，并在 UI 中发送新消息提醒、刷新聊天记录，询问用户选择接收或拒绝。

### transport.FileAcceptedEvent - 收到文件传输许可

此事件表示对端已同意接收文件，应该更新已保存的文件传输信息，并在 UI 中发送发送提示。

### transport.FileRejectedEvent - 收到拒绝文件传输通知

此事件表示对端已拒绝接收文件，应该更新已保存的文件传输信息，并在 UI 中发送发送提示。

## 发送给 UI 的通知：
