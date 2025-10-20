package com.assetcomp.scd2.sink;

import com.assetcomp.scd2.model.AssetChangeEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StarRocksScd2SinkTest {

    @Mock
    private Connection mockConnection;
    @Mock
    private PreparedStatement mockUpdateStatement;
    @Mock
    private PreparedStatement mockInsertStatement;

    private StarRocksScd2Sink sink;
    private ParameterTool params;
    private MockedStatic<DriverManager> mockedDriverManager;

    @BeforeEach
    void setUp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("starrocks.hostname", "localhost");
        properties.setProperty("starrocks.port", "9030");
        properties.setProperty("starrocks.database.name", "test");
        properties.setProperty("starrocks.username", "root");
        properties.setProperty("starrocks.password", "");
        Map<String, String> map = properties.entrySet().stream().collect(
                Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue())));
        params = ParameterTool.fromMap(map);

        sink = new StarRocksScd2Sink(params);

        mockedDriverManager = mockStatic(DriverManager.class);
        mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                .thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString()))
                .thenReturn(mockUpdateStatement)
                .thenReturn(mockInsertStatement);
    }

    @AfterEach
    void tearDown() {
        mockedDriverManager.close();
    }

    @Test
    void testOpen() throws Exception {
        // Act
        sink.open(new Configuration());

        // Assert
        mockedDriverManager.verify(() -> DriverManager.getConnection(
                eq("jdbc:mysql://localhost:9030/test"), eq("root"), eq("")));
        verify(mockConnection, times(1)).prepareStatement(
                eq("UPDATE asset_history SET is_current = false, effective_end_ts = ? WHERE owner_id = ? AND is_current = true"));
        verify(mockConnection, times(1)).prepareStatement(
                eq("INSERT INTO asset_history (owner_id, asset_info, source_id, effective_start_ts, effective_end_ts, is_current, op_type) VALUES (?, ?, ?, ?, ?, ?, ?)"));
    }

    @Test
    void testInvokeCreate() throws Exception {
        // Arrange
        AssetChangeEvent event = new AssetChangeEvent();
        event.setOp("c");
        event.setOwnerId(1);
        event.setAssetInfo("some_asset_info");
        event.setId(100);
        event.setTs_ms(System.currentTimeMillis());

        // Act
        sink.open(new Configuration()); // Call open to initialize statements
        sink.invoke(event, null);

        // Assert
        verify(mockUpdateStatement, times(1)).setTimestamp(anyInt(), any(Timestamp.class));
        verify(mockUpdateStatement, times(1)).setInt(anyInt(), eq(1));
        verify(mockUpdateStatement, times(1)).executeUpdate();

        verify(mockInsertStatement, times(1)).setInt(1, 1);
        verify(mockInsertStatement, times(1)).setString(2, "some_asset_info");
        verify(mockInsertStatement, times(1)).setInt(3, 100);
        verify(mockInsertStatement, times(1)).setTimestamp(anyInt(), any(Timestamp.class));
        verify(mockInsertStatement, times(1)).setBoolean(6, true);
        verify(mockInsertStatement, times(1)).setString(7, "c");
        verify(mockInsertStatement, times(1)).executeUpdate();
    }

    @Test
    void testInvokeUpdate() throws Exception {
        // Arrange
        AssetChangeEvent event = new AssetChangeEvent();
        event.setOp("u");
        event.setOwnerId(2);
        event.setAssetInfo("new_asset_info");
        event.setId(101);
        event.setTs_ms(System.currentTimeMillis());

        // Act
        sink.open(new Configuration()); // Call open to initialize statements
        sink.invoke(event, null);

        // Assert
        verify(mockUpdateStatement, times(1)).executeUpdate();
        verify(mockInsertStatement, times(1)).executeUpdate();
    }

    @Test
    void testInvokeDelete() throws Exception {
        // Arrange
        AssetChangeEvent event = new AssetChangeEvent();
        event.setOp("d");
        event.setOwnerId(3);
        event.setTs_ms(System.currentTimeMillis());

        // Act
        sink.open(new Configuration()); // Call open to initialize statements
        sink.invoke(event, null);

        // Assert
        verify(mockUpdateStatement, times(1)).executeUpdate();
        verify(mockInsertStatement, never()).executeUpdate();
    }

    @Test
    void testClose() throws Exception {
        // Act
        sink.open(new Configuration()); // Call open to initialize statements
        sink.close();

        // Assert
        verify(mockUpdateStatement, times(1)).close();
        verify(mockInsertStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }
}
