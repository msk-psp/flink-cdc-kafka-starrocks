package com.assetcomp.cdc;

import com.assetcomp.cdc.model.AssetChangeEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CdcProducerJobTest {

    private CdcProducerJob.AssetChangeMapper mapper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mapper = new CdcProducerJob.AssetChangeMapper();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testMapCreateEvent() throws Exception {
        // Given: A sample Debezium JSON for a create event
        String createEventJson = "{\"op\":\"c\",\"ts_ms\":1672531200000,\"after\":{\"id\":101,\"owner_id\":123,\"asset_info\":\"some_asset_info\"}}";

        // When: The mapper transforms the event
        String resultJson = mapper.map(createEventJson);

        // Then: The output should be a valid AssetChangeEvent JSON
        AssetChangeEvent resultEvent = objectMapper.readValue(resultJson, AssetChangeEvent.class);

        assertNotNull(resultEvent);
        assertEquals("c", resultEvent.getOp());
        assertEquals(101, resultEvent.getId());
        assertEquals(123, resultEvent.getOwnerId());
        assertEquals("some_asset_info", resultEvent.getAssetInfo());
        assertEquals(1672531200000L, resultEvent.getTs_ms());
    }

    @Test
    void testMapUpdateEvent() throws Exception {
        // Given: A sample Debezium JSON for an update event
        String updateEventJson = "{\"op\":\"u\",\"ts_ms\":1672534800000,\"before\":{\"id\":101,\"owner_id\":123,\"asset_info\":\"old_asset_info\"},\"after\":{\"id\":101,\"owner_id\":123,\"asset_info\":\"new_asset_info\"}}";

        // When: The mapper transforms the event
        String resultJson = mapper.map(updateEventJson);

        // Then: The output should reflect the 'after' state
        AssetChangeEvent resultEvent = objectMapper.readValue(resultJson, AssetChangeEvent.class);

        assertNotNull(resultEvent);
        assertEquals("u", resultEvent.getOp());
        assertEquals(101, resultEvent.getId());
        assertEquals(123, resultEvent.getOwnerId());
        assertEquals("new_asset_info", resultEvent.getAssetInfo());
        assertEquals(1672534800000L, resultEvent.getTs_ms());
    }

    @Test
    void testMapDeleteEvent() throws Exception {
        // Given: A sample Debezium JSON for a delete event
        String deleteEventJson = "{\"op\":\"d\",\"ts_ms\":1672538400000,\"before\":{\"id\":101,\"owner_id\":123,\"asset_info\":\"some_asset_info\"}}";

        // When: The mapper transforms the event
        String resultJson = mapper.map(deleteEventJson);

        // Then: The output should reflect the 'before' state and have a null asset_info
        AssetChangeEvent resultEvent = objectMapper.readValue(resultJson, AssetChangeEvent.class);

        assertNotNull(resultEvent);
        assertEquals("d", resultEvent.getOp());
        assertEquals(101, resultEvent.getId());
        assertEquals(123, resultEvent.getOwnerId()); // Crucially, owner_id should be present
        assertNull(resultEvent.getAssetInfo()); // Asset info is not expected for delete ops
        assertEquals(1672538400000L, resultEvent.getTs_ms());
    }

    @Test
    void testMapInvalidJson() throws Exception {
        // Given: An invalid JSON string
        String invalidJson = "{\"op\":\"c\",";

        // When & Then: The mapper should handle the error gracefully (e.g., return null and log)
        // Note: The current implementation logs the error and returns null.
        String result = mapper.map(invalidJson);
        assertNull(result);
    }
}
