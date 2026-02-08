# ha-monitor

---

## 📋 项目简介

这是一个基于 Python 的轻量级监控方案，通过 **Home Assistant 长期访问令牌 (LLAT)** 访问Home Assistant，并在数值达到预设条件时，通过 **Server酱 (ServerChan)** 发送微信消息推送。

由于使用 LLAT 令牌，脚本中不会明文存储账号密码，且只拥有“只读”权限，与智能家居控制权完全隔离。

Server酱消息推送服务每日免费推送五条信息。详情见https://sct.ftqq.com

---

## ⚙️ 配置说明


1. **Home Assistant LLAT**: 在 HA 个人设置页面底部创建。
2. **Server酱 SendKey**: 从https://sct.ftqq.com获取。

### 关键参数表（example）

| `HA 地址` | HA 访问地址 | `http://192.168.1.10:8123` |
| `LLAT` | 长期访问令牌 | `eyJhbGciOiJIUzI1Ni...` |
| `ServerChan SendKey` | Server酱 SendKey | `SCT12345T...` |
| `ENTITY_ID` | 监控对象 ID | `sensor.bedroom_temp` |
