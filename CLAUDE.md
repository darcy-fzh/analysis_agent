## 项目概述
这是一个部署在阿里云 ECS 上的**企业级数据分析AI Agent**。用户通过自然语言提问，Agent调用阿里百炼里的大模型转换为SQL，查询本地MySQL数据库，返回分析结果和可视化图表。
本项目开发遵循原则是在本地开发，你需要在本地测试代码语法是否正确，然后部署到ECS上运行。ECS里的配置（包括DB，大模型API的信息）都存在env文件里。

**核心价值主张**：让非技术人员也能用自然语言进行复杂数据分析，特别关注**比例指标的正确计算**（如客单价、转化率等）。

## 🎯 MVP里程碑
### Phase 1: 核心管道 (Week 1)
- [ ] 基础自然语言转SQL
- [ ] 简单查询执行
- [ ] Streamlit基础界面

### Phase 2: 指标系统 (Week 2)
- [ ] 比例指标正确计算
- [ ] 指标配置界面
- [ ] 查询历史记录

### Phase 3: 增强功能 (Week 3-4)
- [ ] 多数据源支持
- [ ] 高级可视化
- [ ] 性能优化


## 🔧 技术栈详情
| 组件 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 前端框架 | Streamlit | 1.28+ | 快速构建数据应用界面 |
| AI服务 | 阿里百炼| 最新 | 自然语言转SQL |
| 数据库 | MySQL | 8.0+ | 业务数据存储 |

## 🔐 安全规范
### 核心原则
1. **敏感信息零硬编码**：所有密码、API Key、密钥必须从 `.env` 文件读取
2. **最小权限原则**：数据库连接使用只读账号，API Key使用最小必要权限
3. **防御性编程**：所有用户输入必须验证和清理


# 补充说明

## 1. Agent运行调用阿里百炼API
**回答：**
- **模型**：通义千问系列模型，使用Deepseek V4 pro
- **服务**：阿里百炼（Bailian）平台的模型服务
- **SDK**：使用阿里官方Python SDK - `dashscope`
- **API端点**：`dashscope.aliyuncs.com`
- **参考文档**：
  - 官方文档：https://help.aliyun.com/zh/model-studio/developer-reference/quick-start
  - Python SDK文档：https://help.aliyun.com/zh/model-studio/developer-reference/use-dashscope-sdk
- **API Key获取**：从阿里云百炼控制台创建API Key
- **计费**：按token计费，有免费额度

## 2. MySQL schema
**回答：**
目前还没有数据库，**请帮我创建一个示例零售电商数据库**用于开发测试。

**建议的示例schema：**
```sql
-- 创建数据库
CREATE DATABASE analytics_dev;
USE analytics_dev;

-- 订单表（核心事实表）
CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_date DATE NOT NULL,
    customer_id INT NOT NULL,
    channel VARCHAR(20) NOT NULL,  -- web, app, store
    region VARCHAR(50),  -- 地区
    gmv DECIMAL(10, 2) NOT NULL,  -- 总交易额
    order_amount DECIMAL(10, 2) NOT NULL,  -- 订单金额
    product_category VARCHAR(50),
    payment_method VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 用户表
CREATE TABLE customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE,
    customer_segment VARCHAR(20),  -- VIP, Regular, New
    region VARCHAR(50)
);

-- 产品表
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2)  -- 成本
);

-- 插入示例数据
INSERT INTO orders (order_date, customer_id, channel, region, gmv, order_amount, product_category, payment_method) VALUES
('2024-01-15', 1, 'web', '华东', 100.00, 100.00, 'electronics', 'credit_card'),
('2024-01-15', 2, 'app', '华北', 150.00, 150.00, 'clothing', 'alipay'),
('2024-01-16', 1, 'store', '华东', 200.00, 200.00, 'electronics', 'wechat_pay'),
('2024-01-16', 3, 'web', '华南', 75.00, 75.00, 'books', 'credit_card'),
('2024-01-17', 2, 'app', '华北', 120.00, 120.00, 'clothing', 'alipay'),
('2024-01-18', 4, 'web', '华东', 300.00, 300.00, 'electronics', 'credit_card'),
('2024-01-19', 1, 'store', '华东', 80.00, 80.00, 'accessories', 'cash'),
('2024-01-20', 5, 'app', '华南', 220.00, 220.00, 'electronics', 'alipay');

INSERT INTO customers (id, name, email, registration_date, customer_segment, region) VALUES
(1, '张三', 'zhangsan@example.com', '2023-12-01', 'VIP', '华东'),
(2, '李四', 'lisi@example.com', '2024-01-01', 'Regular', '华北'),
(3, '王五', 'wangwu@example.com', '2024-01-10', 'New', '华南'),
(4, '赵六', 'zhaoliu@example.com', '2023-11-15', 'VIP', '华东'),
(5, '孙七', 'sunqi@example.com', '2024-01-05', 'Regular', '华南');

INSERT INTO products (id, name, category, price, cost) VALUES
(1, 'iPhone 15', 'electronics', 6999.00, 5000.00),
(2, '羽绒服', 'clothing', 899.00, 500.00),
(3, 'Python编程', 'books', 99.00, 30.00),
(4, '蓝牙耳机', 'electronics', 299.00, 150.00),
(5, '运动鞋', 'clothing', 599.00, 300.00);
```

**注意**：这个示例schema包含了典型的零售数据分析场景，特别适合测试GMV per customer等比例指标。

## 3. Current project state
**回答：**
这是一个**全新项目**，目前是空目录，需要从零开始构建，然后commit到我的git仓库


## 4. ECS deployment method
**回答：**
推荐使用**Docker Compose**部署，这是最简洁可靠的方式：

**部署方案**：
```
Docker Compose（推荐）
  - 在ECS上安装Docker和Docker Compose
  - 通过git拉取代码
  - docker-compose up -d 启动服务

## 5. Start from Phase 1?
**回答：**
**是的，请从Phase 1开始**，按照我们之前讨论的MVP里程碑：

**Phase 1核心管道（Week 1-2）**：
1. Streamlit基础界面
2. 连接MySQL数据库
3. 实现自然语言转SQL（通过阿里百炼）
4. 执行查询并展示结果
5. 添加基础的内存缓存

**具体任务优先级**：
1. ✅ 创建项目结构和依赖文件
2. ✅ 实现数据库连接和简单查询
3. ✅ 集成阿里百炼API
4. ✅ 创建Streamlit前端界面
5. ✅ 实现基础的查询管道
6. ✅ 添加错误处理和日志

**注意**：Phase 1先不处理复杂的比例指标计算，只实现基础的查询功能。等Phase 1跑通后，再进入Phase 2的指标系统。

## 补充说明
**开发环境**：
- 不需要Redis（我们用内存缓存替代）

