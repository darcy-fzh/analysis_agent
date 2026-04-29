# 数据分析 AI Agent

基于 LLM 的自然语言数据分析工具。用户用自然语言提问，Agent 自动生成 SQL 查询本地 MySQL 数据库，返回分析结果和可视化图表。

## 技术栈

| 组件 | 技术 |
|------|------|
| UI | Streamlit |
| AI | LLM (Deepseek V4 Pro) |
| 数据库 | MySQL 8.0+ |
| 部署 | Docker Compose |

## 运行方式

```bash
cp .env.example .env   # 填入真实配置
docker compose -f docker-compose.prod.yml up -d --build
```

访问 `http://localhost:8501`
