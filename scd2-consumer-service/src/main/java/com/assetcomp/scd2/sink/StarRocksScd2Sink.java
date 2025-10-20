package com.assetcomp.scd2.sink;

import com.assetcomp.scd2.model.AssetChangeEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Timestamp;

public class StarRocksScd2Sink extends RichSinkFunction<AssetChangeEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksScd2Sink.class);
    private final ParameterTool params;
    private Connection connection;
    private PreparedStatement updateStatement;
    private PreparedStatement insertStatement;
    private Statement stmt;
    private ResultSet rs;

    public StarRocksScd2Sink(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Opening StarRocks connection...");
        Class.forName("com.mysql.cj.jdbc.Driver");
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s",
                params.get("starrocks.hostname"),
                params.get("starrocks.port"),
                params.get("starrocks.database.name"));

        connection = DriverManager.getConnection(jdbcUrl, params.get("starrocks.username"), params.get("starrocks.password"));

        String updateSql = "UPDATE asset_history SET is_current = false, effective_end_ts = ? WHERE owner_id = ? AND is_current = true";
        updateStatement = connection.prepareStatement(updateSql);

        String insertSql = "INSERT INTO asset_history (owner_id, asset_info, source_id, effective_start_ts, effective_end_ts, is_current, op_type) VALUES (?, ?, ?, ?, ?, ?, ?)";
        insertStatement = connection.prepareStatement(insertSql);
        
    }

    @Override
    public void invoke(AssetChangeEvent value, Context context) throws Exception {
        LOG.info("Processing event: {}", value);
        String op = value.getOp();
        Timestamp eventTime = new Timestamp(value.getTs_ms());

        if ("c".equals(op) || "u".equals(op)) {
            // Update existing record
            updateStatement.setTimestamp(1, eventTime);
            updateStatement.setInt(2, value.getOwnerId());
            updateStatement.executeUpdate();

            // Insert new record
            insertStatement.setInt(1, value.getOwnerId());
            insertStatement.setString(2, value.getAssetInfo());
            insertStatement.setInt(3, value.getId());
            insertStatement.setTimestamp(4, eventTime);
            insertStatement.setTimestamp(5, null);
            insertStatement.setBoolean(6, true);
            insertStatement.setString(7, op);
            insertStatement.executeUpdate();
        } else if ("d".equals(op)) {
            // Update existing record for delete
            updateStatement.setTimestamp(1, eventTime);
            updateStatement.setInt(2, value.getOwnerId());
            updateStatement.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing StarRocks connection...");
        if (updateStatement != null) {
            updateStatement.close();
        }
        if (insertStatement != null) {
            insertStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
