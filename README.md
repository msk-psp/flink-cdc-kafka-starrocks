# 실시간 데이터 파이프라인

## 1. 개요

이 프로젝트는 원본 데이터베이스(MySQL)에서 발생하는 아이템 자산 정보의 변경 내역을 실시간으로 추적하여 데이터 웨어하우스(StarRocks)에 적재하고, 특정 시점의 데이터 조회를 가능하게 하는 데이터 파이프라인 구현체입니다.

---

## 2. 데이터 수집 아키텍처

제안된 아키텍처는 변경 데이터 캡처(Change Data Capture, CDC)와 스트림 프로세싱을 기반으로 합니다.

**MySQL → Flink CDC → Kafka → Flink Consumer → StarRocks**

1.  **MySQL & Debezium**: 원본 `item_assets` 테이블의 변경 사항(insert, update, delete)은 데이터베이스의 트랜잭션 로그(Binlog)를 통해 감지됩니다. Flink CDC 커넥터는 내부적으로 Debezium을 사용하여 이 로그를 읽어옵니다.
2.  **Flink CDC Source (`cdc-producer-service`)**: Flink 잡은 MySQL CDC 소스로부터 변경 이벤트를 실시간으로 읽어들입니다. 이 방식은 원본 DB에 부하를 거의 주지 않는 장점이 있습니다.
3.  **Kafka**: Flink가 읽어들인 변경 이벤트는 `item_asset_changes` Kafka 토픽으로 전송됩니다. Kafka는 프로듀서와 컨슈머 사이의 버퍼 역할을 하며, 시스템 간의 결합도를 낮추고 데이터 유실을 방지합니다.
4.  **Flink Consumer (`scd2-consumer-service`)**: 별도의 Flink 잡이 Kafka 토픽의 변경 이벤트를 구독(consume)합니다. 이 컨슈머는 비즈니스 로직을 수행하여 데이터를 데이터 웨어하우스에 맞게 가공합니다.
5.  **StarRocks Sink**: 컨슈머는 가공된 데이터를 최종 목적지인 StarRocks 데이터 웨어하우스에 적재합니다. 여기서는 SCD2(Slowly Changing Dimension Type 2) 형태로 데이터를 저장하여 이력 관리를 수행합니다.

### 아키텍처의 장단점 및 제한사항

-   **장점**:
    -   **실시간 처리**: 데이터 변경이 발생하는 즉시 파이프라인을 통해 전파되어 데이터 최신성을 보장합니다.
    -   **낮은 소스 DB 부하**: 애플리케이션의 수정이나 DB에 대한 직접적인 쿼리 없이 로그를 통해 데이터를 읽어오므로 운영 환경에 미치는 영향이 적습니다.
    -   **확장성 및 안정성**: Flink와 Kafka는 대용량 데이터 처리에 용이하며, 분산 환경에서 높은 처리량과 안정성을 제공합니다.
    -   **시스템 분리**: Kafka를 통해 데이터 생산자와 소비자가 분리되어, 각 시스템이 독립적으로 확장되거나 변경될 수 있습니다.

-   **단점 및 제한사항**:
    -   **복잡성**: Flink, Kafka, Debezium 등 분산 시스템 구성 요소에 대한 깊은 이해가 필요하며, 초기 구축 및 운영 비용이 높습니다.
    -   **데이터 중복 가능성**: Kafka와 Flink의 `at-least-once` 전송 보장 특성상, 장애 발생 시 데이터가 중복 처리될 수 있습니다. 이를 해결하기 위해 컨슈머 측에서 멱등성(idempotency) 처리가 필요합니다.
    -   **CDC 제약**: 소스 DB의 로그(e.g., Binlog)에 접근할 수 있는 권한이 필요하며, DB 버전 및 설정에 따라 CDC 지원 여부가 달라질 수 있습니다.

---

## 3. 데이터 웨어하우스 스키마 및 쿼리

특정 시점의 아이템 자산 정보를 조회하기 위해, 데이터 웨어하우스에는 **SCD Type 2** 모델을 적용한 이력 테이블을 설계합니다.

### StarRocks 테이블 스키마 (`item_asset_history`)

```sql
CREATE TABLE item_asset_history (
    source_id INT NOT NULL COMMENT "원본 item_assets 테이블의 id (PK)",
    owner_id INT NOT NULL COMMENT "소유자를 식별하는 고유 ID",
    effective_start_ts DATETIME NOT NULL COMMENT "이 레코드가 유효하기 시작한 시점",
    asset_info VARCHAR(255) NOT NULL COMMENT "자산 정보",
    effective_end_ts DATETIME NULL COMMENT "이 레코드가 더 이상 유효하지 않게 된 시점",
    is_current BOOLEAN NOT NULL COMMENT "현재 유효한 레코드인지 여부 플래그",
    op_type VARCHAR(1) NOT NULL COMMENT "원본 DB 작업 유형 (c, u, d)"
)
ENGINE = OLAP
PRIMARY KEY(source_id, owner_id, effective_start_ts)
DISTRIBUTED BY HASH(owner_id)
PROPERTIES (
    "replication_num" = "2"
);
```

-   `source_id`: 원본 `item_assets` 테이블의 `id` (PK)
-   `owner_id`: 소유자 ID
-   `asset_info`: 자산 정보
-   `effective_start_ts`: 해당 레코드가 유효하기 시작한 시점
-   `effective_end_ts`: 해당 레코드의 유효성이 끝난 시점. `NULL`일 경우 현재 유효한 레코드임을 의미합니다.
-   `is_current`: 현재 유효한 레코드인지를 나타내는 플래그로, 조회 성능 최적화를 위해 사용됩니다.
-   `op_type`: 원본 데이터베이스에서 발생한 작업 유형 (`c`, `u`, `d`)

### 데이터 생성 로직 (Consumer)

`scd2-consumer-service`는 Kafka로부터 받은 변경 이벤트를 바탕으로 SCD2 테이블을 관리합니다. 이 과정은 단일 쿼리가 아닌, 이벤트에 따라 Flink 애플리케이션이 실행하는 트랜잭션 로직입니다. 각 이벤트에 대한 의사 쿼리(Pseudo-Query)는 다음과 같습니다.

-   **`insert` (`c`)**: 신규 아이템 자산이 등록된 경우
    ```sql
    -- 이벤트 데이터: { id, owner_id, asset_info, registered_at }
    INSERT INTO item_asset_history (source_id, owner_id, asset_info, effective_start_ts, effective_end_ts, is_current, op_type)
    VALUES (event.id, event.owner_id, event.asset_info, event.registered_at, NULL, TRUE, 'c');
    ```

-   **`update` (`u`)**: 기존 아이템 자산 정보가 변경된 경우 (2단계 트랜잭션)
    ```sql
    -- 1. 기존 최신 데이터의 유효기간을 종료시킴
    -- 이벤트 데이터: { owner_id, registered_at }
    UPDATE item_asset_history
    SET effective_end_ts = event.registered_at, is_current = FALSE
    WHERE owner_id = event.owner_id AND is_current = TRUE;

    -- 2. 변경된 정보로 새로운 데이터를 삽입
    -- 이벤트 데이터: { id, owner_id, asset_info, registered_at }
    INSERT INTO item_asset_history (source_id, owner_id, asset_info, effective_start_ts, effective_end_ts, is_current, op_type)
    VALUES (event.id, event.owner_id, event.asset_info, event.registered_at, NULL, TRUE, 'u');
    ```

-   **`delete` (`d`)**: 아이템 자산 정보가 삭제된 경우
    ```sql
    -- 이벤트 데이터: { owner_id, ts_ms } (ts_ms를 삭제 시점으로 사용)
    UPDATE item_asset_history
    SET effective_end_ts = event.delete_timestamp, is_current = FALSE
    WHERE owner_id = event.owner_id AND is_current = TRUE;
    ```

### 특정 시점 조회 쿼리

```sql
-- 특정 소유자(예: owner_id = 123)의 2025년 1월 1일 시점의 자산 정보를 조회하는 쿼리
SELECT asset_info
FROM item_asset_history
WHERE owner_id = 123
  AND '2025-01-01 00:00:00' >= effective_start_ts
  AND ('2025-01-01 00:00:00' < effective_end_ts OR effective_end_ts IS NULL);
```

## 5. 빌드 및 실행

프로젝트를 실행하기 위해 각 Flink 애플리케이션(Producer, Consumer)을 빌드하고, Docker Compose를 사용하여 전체 인프라를 시작한 뒤, Flink 잡을 클러스터에 제출해야 합니다.

### 1. 소스 코드 빌드

각 Flink 애플리케이션을 `jar` 파일로 패키징합니다.

```bash
# Producer 빌드
cd cdc-producer-service
mvn clean package

# Consumer 빌드
cd ../scd2-consumer-service
mvn clean package

# 프로젝트 루트로 복귀
cd ..
```

### 2. 인프라 실행

프로젝트 루트 디렉토리에서 `docker-compose`를 사용하여 모든 서비스(MySQL, Kafka, StarRocks, Flink)를 시작합니다.

```bash
docker-compose up -d
```

### 3. Flink 잡 제출

인프라가 실행된 후, 빌드된 `jar` 파일을 Flink 클러스터에 잡으로 제출합니다.

```bash
# Producer 잡 제출
docker-compose exec flink-jobmanager flink run /opt/flink/usrlib/producer/cdc-producer-service-1.0.0.jar

# Consumer 잡 제출
docker-compose exec flink-jobmanager flink run /opt/flink/usrlib/consumer/scd2-consumer-service-1.0.0.jar
```

이제 모든 파이프라인이 동작하며 MySQL의 `item_assets` 테이블 변경 사항을 감지하여 StarRocks에 이력 데이터로 적재합니다.