# TCP Gateway Service

## Stack
- Java 21
- Spring Boot 4.x
- Reactor Netty (reactor-netty-core) for TCP connection management
- Maven

## Architecture
- Connection pools are managed by `TcpConnectionPoolManager`
- Correlation is handled by `CorrelationRegistry` using `CompletableFuture`
- REST endpoint: `POST /{direction}/{ppg}/ci-to-bank`
- REST endpoint: `POST /{direction}/{ppg}/`
- Pool config driven by `@ConfigurationProperties` from `application.yml`

## Conventions
- Package root: `com.jackbourner.tcpgateway`
- All pools are outbound (service initiates all connections)
- Unsolicited server-push messages route to a separate handler
- No Spring Integration TCP — use Reactor Netty directly

## Build & run
- `mvn spring-boot:run`
- `mvn verify` to run tests
