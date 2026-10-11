# 运行时重启恢复

[English](restart-spool.md)

计划重启时，恢复目录包含两个相互独立的原子责任域：EventBus 权威快照
`eventbus-recovery.spool` 与 MQTT broker 快照 `mqtt-runtime.state`。正常流量不会写入
任何一个文件。Auth cache、网关控制快照和业务离线命令永远不会写入 spool。

## EventBus：NBSP v3

当前 EventBus writer 输出 NBSP v3：

```text
magic "NBSP" | version u32 | generation u64
repeat {
  record length u32 | JSON SpoolRecord | SHA-256 checksum
}
trailer "SEND" | record count u64 | protected byte count u64 | SHA-256
```

最终摘要覆盖从 magic 到 trailer byte count 的所有字节，包括 version、generation、记录
长度、payload、逐记录 checksum 和顺序。protected byte count 是 header 加全部记录 framing/
content；固定 52 字节 trailer 计入 segment/total 容量。reader 要求权威 trailer、精确
count/length 和 digest，并拒绝尾随字节。writer 每次只序列化一条有界记录。

`SpoolRecord` 保存完整规范化事件、稳定 `event_id`、待处理 required sink ID、routing
revision、接纳时间与必要 attempt 元数据。分配/解码前会按配置检查所有文件和记录长度，
包括 record、segment、total bytes 与 record count。未知版本、不完整尾部、checksum
不匹配、随机字节和超长 length 都会明确失败。

提交时在 Unix 的 `0700` 目录内写入私有 `0600` 临时文件，sync 并关闭后原子替换
`eventbus-recovery.spool`，再完成下述平台提交步骤。计划关机只有在这些步骤成功后才能
成功退出。每次替换都递增 generation。新 rename 后、旧清理前发生崩溃时只会选择新
generation；清理前也会检查 generation，因此 stale cleanup handle 不能删除同一路径的
较新 image。遗留 `.tmp` 被忽略。

只接受 NBSP v3。命名文件 `eventbus-recovery.spool` 是唯一权威快照；恢复不会聚合旧
append-only 文件，也不会 fallback 到其他文件名。存在非权威 leftover 但没有命名快照时
会 fail closed，因为 leftover 可能仍拥有已接纳工作。旧版/未知版本会在记录解码前返回
`UnsupportedRecoveryVersion(version)`。未知或不可读 stale file 会在删除权威文件前
阻止清理。升级前保留完整目录，并使用合适旧版本完成或转换责任；见
[当前协议升级](migration/current-protocol-only.zh-CN.md)。Windows ACK 清理会保留已 sync
的空当前格式 successor；这不表示不兼容 reader 可以读取它。

业务已处理事件但 ACK 丢失时，pending record 会用相同 `event_id` 重放，可能造成重复；
因此消费者必须幂等。

## MQTT：NBMQ v6

MQTT 快照使用紧凑 NBMQ v6 typed record：带 checksum 的 version/generation header、
有界 binary record（长度与 checksum），以及最终 record-count、byte-count 和整条流 SHA-256
trailer。session、subscription、QoS state、retained data 与 pending Will 从同一个一致视图
编码，不克隆完整状态。reader 只接受 NBMQ v6；旧版/未知版本明确失败。

独立 `mqtt_recovery_max_bytes` ceiling 覆盖配置允许的 broker state，不继承 EventBus
record limit。两种格式都遵循私有目录、文件 sync 与平台原子替换规则。MQTT 状态和 ACL
恢复细节见 [MQTT 会话恢复](mqtt-session-recovery.md)。

两个文件不宣称形成跨域数据库事务。成功关机前，每个责任域都必须独立完整且可安全
重放。任一提交失败都会让进程保持存活且 unready，并按有界节奏重试；失败的 EventBus
尝试会删除自己的私有临时文件。SIGKILL、OS 崩溃或断电仍可能丢失近期内存变化，不能
描述为 crash durability。

## 不支持的文件

当前 runtime 没有历史 payload decoder、迁移服务或 fallback。NBMQ v1–v5 与 NBSP v1/v2
必须在升级前由合适旧版本完成或转换。不支持的 EventBus spool 会在 listener/readiness
发布前阻断启动，并保留已提交字节。当前格式中畸形 event JSON、长度、checksum 或 trailer
仍属于无效输入。新版本不会跳过责任、重新解释字节或静默删除旧文件。操作步骤见
[破坏性变更升级指南](migration/current-protocol-only.zh-CN.md)。

## 目录所有权与平台 I/O

网关在读取任一恢复域之前取得 `.netbaiot.lock`。OS advisory exclusive lock 由共享 owner
持有，包括 blocking I/O job，直到所有 owner 退出。另一个协作网关即使使用不同端口也会
返回 `Conflict`。释放时不会 unlink lock inode；进程结束会释放锁。

应使用专用、可信的本地文件系统目录（APFS/ext4/NTFS）。网络文件系统、攻击者替换
parent/directory component 不在契约范围。使用原始 broker/spool API 的 library composition
root 必须绑定同一 directory owner；独立 decoder 调用不需要锁。file link、reparse point
和 special file 会被拒绝；Unix 读取还使用 no-follow/nonblocking open。只有在目录本身可
访问时才允许文件不存在。I/O 错误会阻断启动；实际读取长度与目录遍历也有上限，包括
被忽略的临时项。

任一 writer 创建临时文件前，会在共享 `spool_max_records + 16` 目录项预算内预留空间。
因此正常串行关机重试中，unlink 失败不能无限累积临时文件。超额目录项会阻止进一步提交，
直到目录被修复；已提交责任保持不变。

Unix 提交流程为：创建私有临时文件、有界流式写入、`sync_all`、关闭、同目录 rename、
目录 `sync_all`。Windows 使用 `atomicwrites` 0.4.4 调用
`MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)` 替换已 sync/关闭文件，从而避免无效的
只读目录 flush。wrapper 会传播 sharing/ACL 等替换错误，但不额外宣称已证明 NTFS 目录
flush 或断电事务。Windows ACK 清理保留已 sync 的空 NBSP v3 successor；它不同于
`commit([])`（后者 no-op 并保留旧工作）。Unix 清理会在检查 identity/generation 并验证
所有 stale file 后 unlink；未知/不可读 stale file 会在删除权威文件前阻断清理。

`fs2` 0.4.3 提供与 Rust 1.88 兼容的安全 `flock`/Windows `LockFileEx` 所有权；其平台
内部 unsafe 和 `atomicwrites` 的小型 Win32 wrapper 已审查。NetbaIoT 自身没有新增 unsafe
代码。原生恢复/生命周期 CI 位于 `.github/workflows/recovery-platform.yml`；只有配置或
编译不能作为原生执行证据。

平台参考： [Rust 1.88 Windows filesystem 实现](https://github.com/rust-lang/rust/blob/1.88.0/library/std/src/sys/fs/windows.rs)、
[MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)、
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)、
[atomicwrites 实现](https://docs.rs/crate/atomicwrites/0.4.4/source/src/lib.rs)、
[fs2 lock 契约](https://docs.rs/fs2/0.4.3/fs2/trait.FileExt.html)。
