package com.assetcomp.cdc.model;

import java.io.Serializable;

/**
 * 'item_assets' 테이블의 변경 이벤트를 나타내는 데이터 모델 클래스입니다.
 * 이 객체가 직렬화되어 Kafka로 전송됩니다.
 */
public class AssetChangeEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String op;      // 작업 유형 (c: create, u: update, d: delete)
    private Integer id;     // 원본 테이블의 Primary Key (item_assets.id)
    private Integer owner_id;
    private String asset_info;
    private Long ts_ms;     // 이벤트 발생 타임스탬프 (milliseconds)

    /**
     * Flink의 POJO 직렬화를 위한 기본 생성자입니다.
     */
    public AssetChangeEvent() {}

    // Getters and Setters
    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getOwnerId() {
        return owner_id;
    }

    public void setOwnerId(Integer ownerId) {
        this.owner_id = ownerId;
    }

    public String getAssetInfo() {
        return asset_info;
    }

    public void setAssetInfo(String assetInfo) {
        this.asset_info = assetInfo;
    }

    public Long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(Long ts_ms) {
        this.ts_ms = ts_ms;
    }

    @Override
    public String toString() {
        return "AssetChangeEvent{" +
                "op='" + op + "'" + 
                ", id=" + id + 
                ", owner_id=" + owner_id + 
                ", asset_info='" + asset_info + "'" + 
                ", ts_ms=" + ts_ms + 
                '}' ;
    }
}