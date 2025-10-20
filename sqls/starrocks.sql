create database test;

use test;

drop table asset_history;

CREATE TABLE asset_history (
    source_id INT NOT NULL COMMENT "원본 asset 테이블의 id (PK)",
    user_id INT NOT NULL COMMENT "사용자를 식별하는 고유 ID",
    effective_start_ts DATETIME NOT NULL COMMENT "이 레코드가 유효하기 시작한 시점",
    account VARCHAR(255) NOT NULL COMMENT "사용자의 계좌 정보",
    effective_end_ts DATETIME NULL COMMENT "이 레코드가 더 이상 유효하지 않게 된 시점",
    is_current BOOLEAN NOT NULL COMMENT "현재 유효한 레코드인지 여부 플래그",
    op_type VARCHAR(1) NOT NULL COMMENT "원본 DB 작업 유형 (c, u, d)"
)
ENGINE = OLAP
PRIMARY KEY(source_id, user_id, effective_start_ts)
DISTRIBUTED BY HASH(user_id)
PROPERTIES (
    "replication_num" = "2"
);