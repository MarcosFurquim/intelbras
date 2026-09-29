# Decisões técnicas e trade-offs

| Decisão | Motivo | Custo ou limite |
|---|---|---|
| Uma aplicação Spring Boot com importador e consumidor | Reduz configuração e facilita executar o case; responsabilidades ficam em componentes distintos. | Em produção, importação e consumo poderiam escalar e implantar separadamente. |
| CSV importado por chamada explícita | Demonstra o fluxo e evita republicar a cada startup. | Endpoint síncrono espera confirmação de cada publicação; para milhões de eventos, usar processamento em lotes com limite de requisições em voo. |
| `body_completo` como payload de origem | Preserva as diferenças reais entre tipos, evitando reconstrução com perda de campos opcionais. | Exige validação defensiva e contratos versionados ao crescer. |
| Chave Kafka pelo dispositivo (`did` ou `deviceId`) | Mantém afinidade e ordem de publicação por dispositivo. | Dispositivos muito ativos podem desequilibrar partições; não há ordem global nem correção de atraso na origem. |
| Envelope com versão e hash SHA-256 do JSON | Identidade estável para replay da amostra, sem depender de `event_number` como ID de produção. | A origem real deve fornecer ID; hash de payload pode ter falso positivo semântico ou não detectar JSON reordenado. |
| PostgreSQL com `event_id` único | Deduplicação persiste entre restarts; efeito e marcador são a mesma inserção transacional. | Escrita adicional por evento; efeitos externos futuros precisam de idempotência própria ou outbox. |
| Tentativas curtas na partição e DLT | Isola falhas permanentes e mantém ordem de consumo por partição enquanto se tenta recuperar. | Um erro transitório atrasa mensagens da mesma partição; DLT precisa de operação e replay controlado. |
| Seis partições e broker único no Compose | Evidencia consumer groups e roda em um notebook. | Não é configuração de produção ou teste de capacidade. |
| Persistir classificação e resumo, não o JSON original | Demonstra processamento sem replicar o material recebido no banco e mantém inspeção simples. | Não há projeção completa de estado por dispositivo. |
| Logs sem payload e métricas Prometheus | Permite investigar processamento sem despejar dados do evento em logs. | Precisa de retenção, dashboards e alertas externos em produção. |

## Semântica de falhas

O fluxo Kafka → banco opera **pelo menos uma vez**. Após o commit no banco, o offset pode ainda não estar confirmado; uma reentrega é reconhecida pela chave única. O produtor usa idempotência e `acks=all`, que reduzem duplicação no log durante retries do próprio produtor, mas não substituem a deduplicação de negócio no consumidor. A publicação na DLT aguarda sucesso antes de avançar após uma falha definitiva.

O `DefaultErrorHandler` distingue erro definitivo (`IllegalArgumentException`) de exceções recuperáveis. Ele tenta estas últimas mais duas vezes, com 500 ms entre tentativas. A DLT preserva a partição original e recebe os cabeçalhos de erro do Spring Kafka.

## Evolução proposta

- Contrato versionado por tipo de evento e validação de schema na entrada.
- IDs gerados pela origem; política de eventos atrasados e estado por dispositivo se o produto exigir.
- Consumidores dedicados por projeção ou caso de uso, em grupos separados.
- Replay operacional da DLT com auditoria, limite de taxa e correção da causa.
- Teste de carga com distribuição realista de tipos, tamanhos e chaves antes de escolher partições e instâncias.
- TLS, autenticação, autorização, gerenciamento de segredos, políticas de retenção e infraestrutura redundante.
