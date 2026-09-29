# Processamento de eventos IoT com Java e Kafka

Case técnico de Backend Sênior. A aplicação lê o CSV fornecido, publica os eventos no Kafka e os consome de modo assíncrono. Cada evento é classificado pelo tipo e registrado no PostgreSQL. Reprocessamentos não criam novos registros; eventos inválidos vão para uma dead-letter topic (DLT) após a política de tentativa definida.

## Requisitos e escopo

- Java 17, Spring Boot, Apache Kafka, PostgreSQL, Flyway e Maven.
- Docker e Docker Compose para executar a aplicação e a infraestrutura local.
- O CSV recebido contém **100 eventos**, **11 tipos** e JSON válido em todas as linhas. O arquivo não está no Git porque o material do case é confidencial.
- O processamento atual gera um registro classificado e uma descrição curta de cada evento. Não controla dispositivos reais nem chama serviços externos.

| Tipo no CSV | Quantidade | Categoria processada |
|---|---:|---|
| `iotProperty` | 45 | property |
| `online` / `offline` | 18 / 7 | device_status |
| `iotEvent` | 6 | iot_event |
| `videoMotion` / `mobileDetect` / `human` / `smartVideoMotion` / `openCamera` | 12 / 4 / 3 / 1 / 1 | detection |
| `informationTransfer` | 2 | transfer |
| `upgradeFail` | 1 | upgrade |

## Execução local

1. Coloque `case_backend_senior_eventos_sanitizados.csv` em `input/`. O arquivo recebido já está nessa pasta na cópia local do projeto; se clonar o repositório, copie-o novamente.
2. Inicie o ambiente:

```bash
docker compose up --build -d
docker compose ps
```

3. Verifique a saúde e dispare a importação quando desejar:

```bash
curl -fsS http://127.0.0.1:8080/actuator/health
curl -fsS -X POST http://127.0.0.1:8080/api/imports/sample
curl -fsS http://127.0.0.1:8080/api/processing/summary
```

O `POST` aguarda a confirmação de publicação dos 100 eventos e retorna `{"accepted":100,"rejected":0}`. O consumo é assíncrono; repita o `GET` até a soma por tipo chegar a 100. Chamar o `POST` novamente publica os mesmos eventos, mas o banco permanece com 100 registros devido à chave única de idempotência. O contador `case_events_duplicate_total` aumenta.

Métricas estão em `http://127.0.0.1:8080/actuator/prometheus`. Logs:

```bash
docker compose logs -f app
```

Para consultar o resultado sem expor o JSON original:

```bash
docker compose exec postgres psql -U case_user -d case_events -c 'SELECT message_type, count(*) FROM processed_events GROUP BY message_type ORDER BY message_type;'
```

Para parar, use `docker compose down`. `docker compose down -v` **apaga os dados locais** de PostgreSQL; só use quando quiser reiniciar a demonstração do zero.

## Testes

```bash
./mvnw test      # testes unitários; inclui validação opcional dos 100 eventos se o CSV local existir
./mvnw verify    # inclui integração Kafka + PostgreSQL via Testcontainers; requer Docker
```

Também é possível executar a aplicação no host, com Kafka e PostgreSQL no Compose:

```bash
docker compose up -d kafka postgres
./mvnw spring-boot:run
```

O aplicativo **não importa automaticamente** ao iniciar. Esse detalhe evita republicação acidental a cada restart. O endpoint de importação lê apenas o caminho configurado em `CASE_CSV_PATH`; ele não aceita um caminho arbitrário enviado pelo cliente. A API está ligada a `127.0.0.1` na configuração local e não tem autenticação. Não a exponha diretamente como serviço público.

## Falhas e reprocessamento

- Uma linha inválida do CSV é rejeitada com número e motivo no log; as demais continuam.
- Uma falha permanente no consumidor vai à DLT. Erros recuperáveis recebem até duas novas tentativas com pausa de 500 ms.
- A DLT tem o mesmo número de partições que o tópico principal. Para observá-la:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic iot.events.dlt --from-beginning
```

- A inserção no PostgreSQL tem `event_id` único. O offset é confirmado depois que o método transacional termina. Se o processo cair após o commit do banco e antes do commit do offset, a mensagem poderá reaparecer, mas não repetirá o registro.

## Documentação

- [Arquitetura e operação](docs/architecture.md)
- [Decisões e trade-offs](docs/decisions.md)
- [Roteiro de demonstração e apresentação](docs/demo.md)

O repositório não contém o enunciado DOCX nem o CSV recebido. Mantenha o material de entrada privado e confirme com o RH como compartilhar o código e a documentação antes de publicar ou enviar o link.
