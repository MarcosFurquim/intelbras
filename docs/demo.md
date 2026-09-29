# Demonstração e apresentação

## Checagem antes da reunião

```bash
docker compose up --build -d
curl -fsS http://127.0.0.1:8080/actuator/health
./mvnw verify
```

Confirme que o CSV recebido está em `input/` e que a soma de registros em `/api/processing/summary` é zero em ambiente recém-criado ou 100 após uma importação anterior. Não execute `down -v` sem intenção de apagar os dados locais.

## Demonstração de 5 minutos

1. Mostrar o diagrama em `docs/architecture.md` e as 11 categorias observadas no CSV.
2. Chamar `POST /api/imports/sample`; destacar `accepted=100`, `rejected=0`.
3. Chamar `GET /api/processing/summary` até somar 100; mostrar os tipos no PostgreSQL e as métricas em `/actuator/prometheus`.
4. Repetir o `POST`; o total no banco continua 100 e a métrica de duplicatas aumenta.
5. Apontar `EventClassifier`, `EventProcessor`, `KafkaConfiguration` e `EventFlowIT`: tipo desconhecido vai à DLT e o evento seguinte continua. Evite inserir uma falha manual no ambiente da apresentação sem ensaiar antes.

## Roteiro de 50 minutos

- **20 min — arquitetura:** problema e dados heterogêneos; fluxo CSV → Kafka → consumidor → PostgreSQL; chave de partição, consumer group, idempotência, retry/DLT, métricas e proposta de escala.
- **15 min — código:** componentes por responsabilidade, contrato do envelope, validação, handlers dos 11 tipos, transação e testes. Faça a demonstração acima.
- **15 min — perguntas:** preparação para discussões de entrega pelo menos uma vez, ordem por dispositivo, rebalances, partições quentes, eventos atrasados, DLT, banco e alta disponibilidade.

## Respostas que vale ensaiar

- **Por que não “exatamente uma vez”?** O produtor idempotente protege retries do produtor; Kafka e PostgreSQL não formam uma transação única nesta solução. A chave única torna o efeito implementado seguro diante de reentrega.
- **Por que não usar `event_number` como ID?** É um número da amostra, sem garantia de identidade global. O hash permite replay do mesmo conteúdo; o sistema real deveria exigir ID da origem.
- **Por que `did` como chave?** Mantém a ordem de publicação por dispositivo, sem sacrificar todo o paralelismo. `informationTransfer` usa `deviceId`.
- **E se a DLT crescer?** Alertar, classificar causas, corrigir código/dados, reprocessar com controle e registrar cada replay.
- **O que a amostra prova?** Leitura, publicação, processamento dos tipos e tolerância a falhas. Ela não prova throughput de produção.
