# Debezium-connector

Проект для отработки навыков по работе с Kafka Connect и Debezium PostgreSQL.

Компоненты системы:

- migrator - утилита для миграции базы данных
- inserter - создает четырех пользователей и геренирует заказы
- consumer - получает сообщения из брокера

## Запуск проекта

1. Клонируйте репозиторий, установите зависимости:

```
git clone git@github.com:niksmo/debezium-connector.git
cd messaging
go mod download
```

2. Запустите инфраструктуру используя `docker compose`:

```
docker compose up -d
```

3. Скомпилируйте Go-приложения:

```
mkdir bin && \
go build -o ./bin/migrator ./cmd/migrator/. & \
go build -o ./bin/inserter ./cmd/inserter/. & \
go build -o ./bin/consumer ./cmd/consumer/. & \
wait
```

4. Выполните миграции для базы данных:
```
./bin/migrator -dsn postgres-user:postgres-pw@127.0.0.1:5432/customers -m ./migrations
```

5. Сконфигурируйте `pg-connector`:

```
curl -X PUT -H 'Content-Type: application/json' --data @infra/pg-connector.json http://localhost:8083/connectors/pg-connector/config
```

6. Запустите `inserter` и `consumer` в отдельный терминалах, первым должен быть запущен `inserter`:

Первый терминал
```
./bin/inserter
```

Второй терминал
```
./bin/consumer
```

## Проверка функционала

### 1. Consumer получает сообщения из Kafka кластера

- Откройне терминал `consumer` и убедись что сообщения выводятся в терминал


### 2. Debezium PostgreSQL Connector отслеживает изменения в таблицах `users` и `orders`:

- Откройте в браузере http://127.0.0.1:8080/ui/clusters/kraft_cluster_1/all-topics

- Убедитесь что топиков с префиксом `customers` только два: `customers.public.users` и `customers.public.orders`
