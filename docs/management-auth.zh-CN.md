# 管理认证与授权

管理 HTTP 使用独立监听器和管理认证服务，与设备认证缓存完全分离。认证失败返回 401 `unauthenticated`；认证成功但 Scope 或资源不符返回 403 `forbidden`。缺少 Scope 时，现有 `ApiError.required_scope` 返回所需值。资源越权只返回通用错误，不透露设备是否存在。

## 认证方式

| 方式 | 请求 | 配置 |
|---|---|---|
| 旧版引导令牌 | `Authorization: Bearer <64 位十六进制>` | `NETBAIOT_ADMIN_SECRET`，默认兼容 |
| API Key | `Authorization: ApiKey <key_id>.<64 位十六进制 secret>` | `management_auth.api_keys` 的 `secret_env` |
| JWT Access Token | `Authorization: Bearer <三段 JWT>` | `management_auth.jwt` 与 HTTPS JWKS URL |
| 管理 mTLS | 经 CA 验证的客户端证书，不带 Authorization | `management_tls` 与证书 SHA-256 显式映射 |

每个请求只能提供一种凭据；客户端证书与 Authorization 头同时出现时拒绝请求，认证失败也不回退到其他 Provider。旧引导令牌映射成 `bootstrap-admin`、`admin.*`、Global，可执行所有管理操作。若它与可用的受限 Provider 同时启用，服务会输出警告。迁移完成后在 `management_auth` 下设置 `"legacy_static_token_enabled": false`。非本机管理监听器必须有可用的 Provider；禁用或过期的 API Key 不算可用。

API Key 配置示例。密钥由环境变量提供，不能明文放进 JSON：

```json
{
  "management_auth": {
    "api_keys": [{
      "key_id": "backend-prod",
      "secret_env": "NETBAIOT_API_KEY_BACKEND",
      "subject": "service:backend",
      "scopes": ["connection.read", "device.command"],
      "resources": {"tenants": ["tenant-a"]},
      "global": false,
      "expires_at": null,
      "auth_generation": 1,
      "enabled": true
    }]
  }
}
```

环境变量必须在启动时存在，值为高熵 64 位十六进制字符串。`expires_at` 使用 Unix 毫秒；禁用或过期的 API Key 认证失败，也不能满足公网监听器的 Provider 检查。`global: true` 必须显式配置；空资源列表不授权任何资源。Key ID、subject、Scope 和资源数量/字节在启动时验证。保留的 `auth_generation` 字段仅为元数据，修改它不会在运行时吊销管理 API Key；吊销时应禁用或移除密钥并重启。

JWT 配置示例：

```json
{
  "management_auth": {
    "jwt": {
      "issuer": "https://identity.example.com/realms/iot",
      "audience": "netbaiot",
      "jwks_url": "https://identity.example.com/realms/iot/protocol/openid-connect/certs",
      "subject_claim": "sub",
      "scope_claim": "scope",
      "roles_claim": "roles",
      "tenant_claim": "tenants",
      "role_scopes": {"iot-operator": ["runtime.read", "connection.read", "device.command"]},
      "global_roles": []
    }
  }
}
```

本阶段只支持 RS256。签名、配置的 `iss`/`aud`、`exp`、`nbf` 都必须有效；拒绝 JWT 自带的 JWK/密钥 URL。JWKS 只从配置的 HTTPS URL 获取，连接超时 2 秒、总超时 5 秒，不跟随重定向，并限制响应字节数、key 数量、TTL 和最短刷新间隔。刷新只有一个执行者，不排无限等待队列。缓存未过期的 key 可在短暂网络故障时使用；过期后关闭。无效 JWT 凭据返回 401；JWKS 故障、超时、不可用的 keyset，或无可验证 key 时的刷新竞争返回 503，且不会放行 token。有效且已刷新 keyset 中的未知 `kid` 返回 401，随机 `kid` 不能持续触发远程请求。Scope claim 支持空格分隔字符串或数组；角色映射由配置决定，只有配置在 `global_roles` 中的角色授予 Global，其他主体按 Tenant claim 限权。

管理 mTLS 使用独立 TLS 配置：

```json
{
  "management_tls": {
    "certificate": "certs/management.pem",
    "private_key": "certs/management-key.pem",
    "client_ca": "certs/client-ca.pem",
    "require_client_certificate": true
  },
  "management_auth": {
    "mtls_identities": [{
      "certificate_sha256": "<64 位十六进制指纹>",
      "subject": "service:iot-platform",
      "scopes": ["runtime.read"],
      "global": true
    }]
  }
}
```

TLS 握手先验证客户端 CA、证书有效期和证书链，再匹配叶证书 DER 的 SHA-256 指纹。未映射证书拒绝。Subject CN 不是权威身份。旧 `tls` 配置仍适用于设备监听器；未配置 `management_tls` 时，管理监听器单独加载同一服务端证书以保持旧配置兼容。`require_client_certificate: true` 要求至少有一条 `mtls_identities` 映射，并使管理监听器只接受 mTLS：每条连接都要提供证书，且不能叠加 Authorization 凭据。此设置不影响 MQTT/TCP 设备。本阶段尚不支持 URI/DNS SAN 映射。

## 授权

Scope：`runtime.read`、`metrics.read`、`connection.read`、`device.command`、`auth.invalidate`、`auth.invalidate.all`、`control.read`、`control.write`、`routes.read`、`routes.write`、`runtime.drain`、`admin.*`（所有 Scope）。资源范围为 Global，或 Tenant、`(tenant_id, product_id)`、完整 `DeviceKey` 的有界并集。`admin.*` 不绕过资源范围。

`control.read` 与 `routes.read` 是保留名称，目前没有对应的读取 API，授予它们不会产生实际权限。

| 接口 | Scope | 资源 |
|---|---|---|
| health、ready、status | `runtime.read` | 无 |
| metrics | `metrics.read` | 无 |
| connections、设备连接查询 | `connection.read` | 列表过滤或完整 DeviceKey |
| 设备命令 | `device.command` | 完整 DeviceKey |
| auth invalidate Device/Product/Tenant | `auth.invalidate` | 对应资源 |
| auth invalidate CredentialVersion/AuthGeneration | `auth.invalidate` | Global |
| auth invalidate All | `auth.invalidate.all` | Global |
| control snapshot | `control.write` | Global |
| routes | `routes.write` | Global |
| drain | `runtime.drain` | Global |

`/connections` 在有界活动会话表内先授权过滤，再做 offset/limit 分页。带请求体的操作先检查 Scope、再解析有界请求体、最后检查资源，之后才修改运行时。

## 迁移与调用

现有 API 路径、`NETBAIOT_ADMIN_SECRET`、Rust Client `.token()`、CLI `--token` 和 `NETBAIOT_TOKEN` 继续可用。逐步配置 API Key/JWT 和最小 Scope/资源范围，迁移后关闭引导令牌。非本机管理入口使用 TLS。不要把密钥写进 JSON、日志或指标标签。

```bash
curl -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET" http://127.0.0.1:9090/api/v1/status
curl -H "Authorization: ApiKey $NETBAIOT_API_KEY" https://gateway.example.com/api/v1/status
curl -H "Authorization: Bearer $ACCESS_TOKEN" https://gateway.example.com/api/v1/status
```

HMAC 请求签名、OAuth2 Token Introspection、管理审计持久化、URI/DNS SAN 身份映射尚未实现。网关不管理用户、密码或会话数据库。

## 资源上限

`limits` 中新增的默认值：`management_auth_max_subject_bytes=128`、`management_auth_max_scopes=32`、`management_auth_max_scope_bytes=512`、`management_auth_max_resource_entries=128`、`management_auth_max_resource_bytes=8192`、`management_api_key_max_entries=128`、`management_api_key_max_bytes=32768`、`management_jwks_max_keys=32`、`management_jwks_max_bytes=65536`、`management_jwks_ttl_ms=300000`、`management_jwks_refresh_min_interval_ms=30000`、`management_jwt_max_bytes=16384`。HTTP 请求仍受现有头、体和并发上限约束。

连接接入与管理 HTTP 请求分别使用有界的限流窗口，沿用 `requests_per_second` 和 `requests_per_ip_second` 配置。每个已接入 HTTP 请求只消耗一次请求额度。

Business RPC V2 使用独立的 mTLS principal 或回环开发 token，管理凭据不能授权它。管理 HTTP 与 V2 的 `auth.invalidate` 共用完整的入口/MQTT 失效边界，详见 [Business RPC Stream V2](business-rpc-v2.zh-CN.md)。
