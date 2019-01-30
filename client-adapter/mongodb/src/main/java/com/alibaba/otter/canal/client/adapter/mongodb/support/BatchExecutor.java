package com.alibaba.otter.canal.client.adapter.mongodb.support;

import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchExecutor implements Closeable {

    private MongoClient mongoClient;

    public BatchExecutor(MongoClient mongoClient) throws SQLException{
        this.mongoClient = mongoClient;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            try {
                if (mongoClient != null) {
                    mongoClient.close();
                }
            } catch (Exception e) {
            }
        }
    }
}
