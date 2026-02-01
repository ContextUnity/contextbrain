# Multi-Service Architecture: Running Brain with Multiple Clients

## 🏗️ Архітектура

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ContextBrain gRPC Server                         │
│                         (Single instance, port 50051)                    │
│                                                                         │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐           │
│  │ BrainService    │ │ CommerceService │ │ Future Services │           │
│  │ (Knowledge, KG) │ │ (Products)      │ │                 │           │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘           │
│                              │                                          │
│                    ┌─────────┴─────────┐                               │
│                    │   PostgreSQL      │                               │
│                    │   (brain schema)  │                               │
│                    └───────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────┘
                          ▲ gRPC (port 50051)
          ┌───────────────┼───────────────┬───────────────┐
          │               │               │               │
    ┌─────┴─────┐   ┌─────┴─────┐   ┌─────┴─────┐   ┌─────┴─────┐
    │ Commerce  │   │ PinkPony  │   │ Router    │   │ Worker    │
    │ (Django)  │   │ (Temporal)│   │ (LangGraph)│  │ (Temporal)│
    │           │   │           │   │           │   │           │
    │ Own DB:   │   │ Uses Brain│   │ Uses Brain│   │ Uses Brain│
    │ commerce  │   │ only      │   │ only      │   │ only      │
    └───────────┘   └───────────┘   └───────────┘   └───────────┘
```

## 🔑 Ключові принципи

### 1. Brain — єдиний gRPC сервіс
- **Один екземпляр** Brain обслуговує **всіх клієнтів**
- Brain НЕ конкурує за порти з іншими сервісами
- Brain використовує свою власну БД (`brain` schema)

### 2. Tenant Isolation через ContextToken
Кожен запит містить `tenant_id` для ізоляції даних:

```python
# Commerce запит
await client.search(tenant_id="traverse", query="...", ...)

# PinkPony запит  
await client.search(tenant_id="pinkpony", query="...", ...)
```

### 3. Connection Pools
Brain використовує `AsyncConnectionPool` з psycopg:
- `pool_min_size=5`
- `pool_max_size=20`

Це дозволяє обслуговувати паралельні запити від різних сервісів.

## 📦 Варіанти розгортання

### Варіант A: Окремі бази даних (Поточний)

```yaml
# .env для Brain
BRAIN_DATABASE_URL=postgresql://brain:pass@localhost:5433/brain

# .env для Commerce
DATABASE_URL=postgresql://commerce:pass@localhost:5432/commerce
```

**Конфлікт портів:** Brain на 5433, Commerce на 5432

### Варіант B: Єдина БД з Schema Isolation (Рекомендований)

```yaml
# .env для Brain
BRAIN_DATABASE_URL=postgresql://brain:pass@localhost:5432/contextunity
# Автоматично: SET search_path TO brain,public

# .env для Commerce  
DATABASE_URL=postgresql://commerce:pass@localhost:5432/contextunity
# Django: OPTIONS = {'options': '-c search_path=commerce,public'}
```

**Переваги:**
- Один PostgreSQL instance
- Немає конфлікту портів
- Спільні extensions (pgvector, ltree)

## 🚀 Запуск без конфліктів

### Локальна розробка (окремі термінали)

```bash
# Terminal 1: Infrastructure
cd projects/traverse && mise run dev

# Terminal 2: Brain gRPC (порт 50051)
cd contextbrain && uv run python -m contextbrain.service

# Terminal 3: Commerce Django (порт 8000)
cd contextcommerce && uv run python manage.py runserver

# Terminal 4: Commerce Worker (Temporal)
cd projects/traverse && mise run commerce_worker

# Terminal 5: Gardener (optional)
cd projects/traverse && mise run gardener
```

### Docker Compose (все разом)

```bash
cd projects/traverse
docker-compose up -d
```

Сервіси автоматично використовують внутрішню мережу Docker:
- `brain:50051` (не конфліктує з host)
- `redis:6379`
- `temporal:7233`

## ⚙️ Конфігурація сервісів

### Brain Service (.env)
```bash
# Обов'язково
BRAIN_DATABASE_URL=postgresql://brain:pass@localhost:5432/contextunity
BRAIN_PORT=50051

# Embeddings
EMBEDDER_TYPE=openai
OPENAI_API_KEY=sk-...
```

### Commerce Service (.env)
```bash
# Django DB
DATABASE_URL=postgresql://commerce:pass@localhost:5432/contextunity

# Brain connection (gRPC mode)
CONTEXT_BRAIN_URL=localhost:50051
CONTEXT_BRAIN_MODE=grpc

# Temporal
TEMPORAL_URL=localhost:7233

# Optional: вимкнути auto-sync для bulk operations
COMMERCE_BRAIN_SYNC_ENABLED=true
```

### PinkPony / Router (.env)
```bash
# Тільки Brain connection
CONTEXT_BRAIN_URL=localhost:50051
```

## 🔒 Безпека

### Connection limits
Налаштуйте PostgreSQL для багатьох клієнтів:

```sql
ALTER SYSTEM SET max_connections = 200;
```

### Connection timeout
Brain connection pool має timeout 60s:
```python
AsyncConnectionPool(dsn, timeout=60.0, ...)
```

## 📊 Моніторинг

Brain логує всі запити:
```
INFO brain:search tenant=traverse query="winter jacket" results=5
INFO commerce:upsert_dealer dealer=vysota sku=ABC123
```

## 🐛 Troubleshooting

### "Connection refused" на порту 50051
```bash
# Перевірте чи Brain запущений
lsof -i :50051

# Якщо порт зайнятий іншим процесом
sudo fuser -k 50051/tcp
```

### "Database connection error"
```bash
# Перевірте доступність PostgreSQL
psql $BRAIN_DATABASE_URL -c "SELECT 1"
```

### "Too many connections"
```bash
# Перевірте активні з'єднання
psql -c "SELECT count(*) FROM pg_stat_activity"
```
