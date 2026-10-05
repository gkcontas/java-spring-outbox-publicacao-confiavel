# Transactional Outbox

Serviço de catálogo que grava no PostgreSQL e publica no Kafka sem nunca sair de sincronia, usando o padrão transactional outbox.

O problema que ele resolve é a **escrita dupla**: gravar no banco e publicar no broker são duas operações em dois sistemas, e nenhuma transação cobre as duas. O `@Transactional` do Spring protege apenas o banco — o `send` para o Kafka acontece fora dele, aconteça o que acontecer com a transação.

As duas saídas ingênuas falham de formas opostas:

| Ordem | O que acontece |
|---|---|
| Publica e depois commita | A transação faz rollback e o evento já saiu: o consumidor reage a um dado que não existe |
| Commita e depois publica | O processo morre no meio: o dado existe e ninguém foi avisado, para sempre |

O outbox troca isso por um problema que o banco resolve sozinho: o evento é gravado **na mesma transação** do dado, numa tabela de saída, e um publicador separado leva essa tabela para o broker.

## Como funciona

```
POST /products
      │
      ▼
  uma transação ──► tabela products
                └─► tabela outbox_event (PENDING)
      │
      ▼  commit
                         publicador agendado
                         SELECT ... FOR UPDATE SKIP LOCKED
                                  │
                                  ▼
                            Kafka (chave = id do produto)
                                  │
                                  ▼
                         consumidor idempotente ──► projeção
```

A projeção é mantida **só por evento**. Nada do lado do catálogo escreve nela, então o que se perde na publicação fica visivelmente faltando lá.

## Tecnologias e bibliotecas

| | |
|---|---|
| Linguagem | Java 21 |
| Framework | Spring Boot 3.5 |
| Mensageria | Apache Kafka via Spring Kafka |
| Persistência | Spring Data JPA, PostgreSQL 16 |
| Migrations | Flyway |
| Validação | Bean Validation |
| Build | Gradle Kotlin DSL (wrapper `gradlew`) |
| Testes | JUnit 5, AssertJ, Awaitility, Testcontainers (Kafka e PostgreSQL) |
| Apoio | Lombok nas entidades |

## Pré-requisitos

- JDK 21 ou superior
- Docker

## Como rodar

```bash
docker compose up -d
```

```bash
./gradlew bootRun
```

A API fica em `http://localhost:8080`. O Flyway cria o schema, e o publicador começa a rodar a cada segundo.

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/products` | Cria um produto e grava o evento no outbox |
| `PUT` | `/products/{id}/price` | Altera o preço e grava o evento |
| `GET` | `/products` | Catálogo (fonte da verdade) |
| `GET` | `/projection` | Visão downstream, construída só por evento |
| `GET` | `/outbox/summary` | Quantos eventos pendentes, publicados e parados |
| `GET` | `/outbox?status=PENDING` | Detalhe dos eventos, com tentativas e último erro |
| `POST` | `/outbox/flush` | Publica um lote na hora, sem esperar o agendamento |
| `POST` | `/outbox/retry-failed` | Devolve os eventos parados para a fila |
| `PUT` | `/products/{id}/price-then-fail` | Altera o preço e falha, para mostrar o rollback |
| `POST` | `/unsafe/publish-then-rollback` | A escrita dupla, publicando antes do commit |
| `POST` | `/unsafe/commit-then-crash` | A escrita dupla, commitando sem publicar |

## Exemplos de uso

Fluxo normal:

```bash
curl -s -X POST localhost:8080/products -H 'Content-Type: application/json' \
  -d '{"sku":"KB-001","name":"Teclado mecanico","price":450.00}'
```

```bash
curl -s localhost:8080/outbox/summary
```

```json
{"pending":0,"published":1,"failed":0}
```

```bash
curl -s localhost:8080/projection
```

```json
[{"productId":"51337b74-...","sku":"KB-001","name":"Teclado mecanico","price":450.00}]
```

Mudança de preço, propagada em segundos:

```bash
curl -s -X PUT localhost:8080/products/<id>/price -H 'Content-Type: application/json' \
  -d '{"price":399.90}'
```

Operação que falha depois de gravar o evento — nada é publicado, e o preço no catálogo não muda:

```bash
curl -s -X PUT localhost:8080/products/<id>/price-then-fail -H 'Content-Type: application/json' \
  -d '{"price":1.00}'
```

## A escrita dupla, medida

O endpoint ingênuo publica dentro da transação e em seguida faz rollback:

```bash
curl -s -X POST localhost:8080/unsafe/publish-then-rollback -H 'Content-Type: application/json' \
  -d '{"sku":"GHOST-01","name":"Produto fantasma","price":99.00}'
```

```bash
curl -s localhost:8080/products    # catálogo:  KB-001
curl -s localhost:8080/projection  # downstream: KB-001, GHOST-01
```

O catálogo tem um produto, a projeção tem dois. `GHOST-01` existe downstream e em lugar nenhum — permanentemente, porque nenhuma correção será enviada.

O outro endpoint ingênuo mostra o inverso: o produto é gravado e o evento nunca sai. O dado está certo e é invisível para todo mundo, sem nada registrado que indique que havia algo a reenviar.

## Broker fora do ar

Congelando o container do Kafka e criando três produtos:

```bash
docker compose pause kafka
```

```bash
curl -s localhost:8080/outbox/summary
```

```json
{"pending":3,"published":2,"failed":0}
```

```bash
curl -s "localhost:8080/outbox?status=PENDING"
```

```
ProductCreated   tentativas=3  KafkaProducerException: Failed to send
ProductCreated   tentativas=4  KafkaProducerException: Failed to send
ProductCreated   tentativas=4  KafkaProducerException: Failed to send
```

O catálogo continua com os quatro produtos: a operação de negócio nunca tocou o broker. Ao voltar, os eventos fluem e a projeção se alinha:

```bash
docker compose unpause kafka
```

## At-least-once, e o que isso exige

O outbox garante que o evento **sai**. Não garante que sai uma vez só — o publicador pode enviar com sucesso e morrer antes de marcar a linha, e a próxima execução envia de novo. Por isso o consumidor guarda os ids já processados e a segunda entrega vira um no-op.

Há um detalhe que aparece na prática e surpreende: **uma publicação que falhou pode ter sido entregue**. Durante o teste de broker congelado, um evento esgotou as cinco tentativas e terminou em `FAILED` — e o produto dele estava na projeção. O broker aceitou o registro e o reconhecimento não voltou a tempo, então o publicador registrou falha. Reenfileirar esse evento produz uma duplicata, que o consumidor descarta pelo id:

```bash
curl -s -X POST localhost:8080/outbox/retry-failed
# {"requeued":1}
```

`FAILED` significa "parou de tentar", nunca "descartado": a linha e o payload continuam lá, e requeue é decisão de operador — o que fez o evento falhar cinco vezes dificilmente se resolveu sozinho.

## Duas instâncias do publicador

A query de reivindicação usa `FOR UPDATE SKIP LOCKED`:

```sql
SELECT ... FROM outbox_event
 WHERE status = 'PENDING' AND next_attempt_at <= now
 ORDER BY created_at
   FOR UPDATE SKIP LOCKED
```

Sem o lock, duas instâncias leem as mesmas linhas e publicam tudo em duplicidade. Com um `FOR UPDATE` comum e sem `SKIP LOCKED`, a segunda instância fica bloqueada esperando a primeira em vez de pegar outro trabalho. O teste `ConcurrentPublisherIntegrationTest` roda dois publicadores sobre 12 eventos e confirma que, juntos, entregam exatamente 12.

## Ordem das mensagens

A chave da mensagem é o id do produto. Kafka só garante ordem **dentro de uma partição**, e a chave é o que decide a partição — então dois eventos do mesmo produto nunca chegam trocados, e eventos de produtos diferentes podem chegar em qualquer ordem, que é o que qualquer consumidor sensato já espera.

## Testes

```bash
./gradlew test
```

16 testes, todos contra Kafka e PostgreSQL reais em containers — não há como testar este padrão com um broker simulado, já que o assunto é justamente o que acontece quando o banco commita e o broker não.

| Classe | Testes | O que cobre |
|---|---|---|
| `OutboxFlowIntegrationTest` | 7 | Gravação transacional, entrega, mudança de preço, rollback, duplicata |
| `DualWriteIntegrationTest` | 3 | Os dois modos de falha da escrita dupla, e o outbox mantendo os dois lados coerentes |
| `BrokerOutageIntegrationTest` | 2 | Broker congelado, acúmulo sem perda, retomada, e o estado de parada |
| `ConcurrentPublisherIntegrationTest` | 1 | Dois publicadores simultâneos entregando cada evento uma vez |
| `OutboxRecorderIntegrationTest` | 3 | `Propagation.MANDATORY`, serialização e descarte no rollback |

## Alternativa: CDC

Em vez de um publicador que faz polling, dá para ler o **log de replicação** do banco com uma ferramenta de change data capture — Debezium é a mais comum. O evento continua sendo gravado na mesma transação; o que muda é quem o leva para o broker.

| | Publicador com polling | CDC no log |
|---|---|---|
| Latência | Intervalo do agendamento | Praticamente imediata |
| Carga no banco | Uma query por intervalo | Nenhuma consulta; lê o WAL |
| Operação | Está dentro da aplicação | Mais um componente para operar |
| Acoplamento | A aplicação decide o que publicar | A ferramenta lê o que foi escrito |

Para a maioria dos sistemas o publicador por polling é suficiente e muito mais simples de operar. CDC compensa quando a latência importa, quando o volume torna o polling caro, ou quando já existe um Debezium no ambiente.

## Relação com o projeto 20

O [monólito modular](../java-spring-modulith-monolito-modular) usa o registro de publicação de eventos do Spring Modulith, que é a mesma ideia **dentro do processo**: a entrega para cada listener é gravada antes e marcada depois. Este projeto é a versão que atravessa a fronteira do processo, com um broker no meio — e com os problemas extras que isso traz: ordenação por partição, duplicata na rede e um broker que pode sumir.

## Decisões de projeto

- **`Propagation.MANDATORY` no gravador.** Ele se recusa a rodar sem uma transação aberta. `REQUIRED` criaria uma e esconderia o erro, devolvendo o sistema à escrita dupla com passos extras.
- **Envio síncrono no publicador.** Esperar o `ack` antes de marcar a linha como publicada. Um envio assíncrono comitaria "publicado" para mensagens que o broker nunca confirmou.
- **Backoff exponencial.** O publicador roda num timer: sem backoff, um broker fora do ar vira um laço de retry apertado contra todas as linhas pendentes, a cada segundo, por toda a duração da queda.
- **Índice parcial.** `WHERE status = 'PENDING'` no índice, porque a única consulta quente é a dos pendentes e a tabela cresce com todo evento que o sistema já emitiu.
- **Eventos são records, não entidades.** Serializar a entidade colocaria nomes de coluna e associações preguiçosas num contrato do qual outros times dependem.
