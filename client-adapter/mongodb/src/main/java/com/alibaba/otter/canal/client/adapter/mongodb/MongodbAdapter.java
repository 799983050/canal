package com.alibaba.otter.canal.client.adapter.mongodb;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.mongodb.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.monitor.MongodbConfigMonitor;
import com.alibaba.otter.canal.client.adapter.mongodb.service.MongodbEtlService;
import com.alibaba.otter.canal.client.adapter.mongodb.service.MongodbMirrorDbSyncService;
import com.alibaba.otter.canal.client.adapter.mongodb.service.MongodbSyncService;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * RDB适配器实现类
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
@SPI("mongodb")
public class MongodbAdapter implements OuterAdapter {

    private static Logger                           logger              = LoggerFactory.getLogger(MongodbAdapter.class);

    private Map<String, MappingConfig>              rdbMapping          = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();                // 库名-表名对应配置
    private Map<String, MirrorDbConfig>             mirrorDbConfigCache = new ConcurrentHashMap<>();                // 镜像库配置

    private DruidDataSource                         dataSource;
    private MongodbSyncService mongodbSyncService;
    private MongodbMirrorDbSyncService mongodbMirrorDbSyncService;

    private MongodbConfigMonitor mongodbConfigMonitor;

    private MongoClient mongoClient;

    public MongoClient getMongoClient() {
        return mongoClient;
    }
    /**
    * 读写分离
    */
    private String readPreference = "PRIMARY";
    public MongoClient mongoClient(OuterAdapterConfig configuration) {
        Map<String, String> properties = configuration.getProperties();
        String hostports = properties.get("mongo.core.replica.set");
        String username = properties.get("mongo.core.username");
        String password = properties.get("mongo.core.password");
        String database = properties.get("mongo.core.dbname");

        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
        build.connectionsPerHost(15000);
        build.threadsAllowedToBlockForConnectionMultiplier(Integer.valueOf(8));
        build.connectTimeout(60000);
        build.maxWaitTime(15000);
        build.readPreference(ReadPreference.valueOf(readPreference));
        MongoClientOptions options = build.build();

        try {
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            for (String hostport : hostports.split(", *")) {
                if (StringUtils.isBlank(hostport)) {
                    continue;
                }
                hostport = hostport.trim();

                ServerAddress serverAddress = new ServerAddress(hostport.split(":")[0],Integer.valueOf(hostport.split(":")[1]));
                addrs.add(serverAddress);
            }

            MongoCredential credential = MongoCredential.createScramSha1Credential(username, database, password.toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);

            mongoClient = new MongoClient(addrs,credentials, options);

            logger.info("【mongodb client】: mongodb客户端创建成功");
        } catch (Exception e) {
            logger.error("【mongodb client】: mongodb客户端创建成功");
            e.printStackTrace();
        }
        return mongoClient;
    }
    /**
     *  连接  库
     */
    public MongoDatabase mongoDatabase(MongoClient mongoClient,String database) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        return mongoDatabase;
    }

//    public MongoCollection<Document> mongoCollection(MongoDatabase mongoDatabase) {
//        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
//        return mongoCollection;
//    }
    public Map<String, MappingConfig> getRdbMapping() {
        return rdbMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    public Map<String, MirrorDbConfig> getMirrorDbConfigCache() {
        return mirrorDbConfigCache;
    }

    /**
     * 初始化方法
     *
     * @param configuration 外部适配器配置信息
     */
    @Override
    public void init(OuterAdapterConfig configuration) {
        Map<String, MappingConfig> rdbMappingTmp = ConfigLoader.load();
        // 过滤不匹配的key的配置
        rdbMappingTmp.forEach((key, mappingConfig) -> {
            if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                || (mappingConfig.getOuterAdapterKey() != null && mappingConfig.getOuterAdapterKey()
                    .equalsIgnoreCase(configuration.getKey()))) {
                rdbMapping.put(key, mappingConfig);
            }
        });

        if (rdbMapping.isEmpty()) {
            throw new RuntimeException("No rdb adapter found for config key: " + configuration.getKey());
        }

        for (Map.Entry<String, MappingConfig> entry : rdbMapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            if (!mappingConfig.getDbMapping().getMirrorDb()) {
                String key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                             + mappingConfig.getDbMapping().getDatabase() + "."
                             + mappingConfig.getDbMapping().getTable();
                Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                    k1 -> new ConcurrentHashMap<>());
                configMap.put(configName, mappingConfig);
            } else {
                // mirrorDB
                String key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                             + mappingConfig.getDbMapping().getDatabase();
                mirrorDbConfigCache.put(key, MirrorDbConfig.create(configName, mappingConfig));
            }
        }

        // 初始化连接池
        Map<String, String> properties = configuration.getProperties();
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setUseUnfairLock(true);

        /**
         *  初始化 mongodb  连接信息
         */
        mongoClient = mongoClient(configuration);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        // String commitSize = properties.get("commitSize");

        boolean skipDupException = BooleanUtils.toBoolean(configuration.getProperties()
            .getOrDefault("skipDupException", "true"));
        mongodbSyncService = new MongodbSyncService(dataSource,
            threads != null ? Integer.valueOf(threads) : null,
            skipDupException);

        mongodbMirrorDbSyncService = new MongodbMirrorDbSyncService(mirrorDbConfigCache,
            dataSource,
            threads != null ? Integer.valueOf(threads) : null,
            mongodbSyncService.getColumnsTypeCache(),
            skipDupException);

        mongodbConfigMonitor = new MongodbConfigMonitor();
        mongodbConfigMonitor.init(configuration.getKey(), this);
    }
    /**
     * 同步方法
     *
     * @param dmls 数据包
     */
    @Override
    public void sync(List<Dml> dmls) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<Boolean> future1 = executorService.submit(() -> {
            mongodbSyncService.sync(mappingConfigCache, dmls);
            return true;
        });
        Future<Boolean> future2 = executorService.submit(() -> {
            mongodbMirrorDbSyncService.sync(dmls);
            return true;
        });
        try {
            future1.get();
            future2.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ETL方法
     *
     * @param task 任务名, 对应配置名
     * @param params etl筛选条件
     * @return ETL结果
     */
    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = rdbMapping.get(task);
        if (config != null) {
            DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (srcDataSource != null) {
                return MongodbEtlService.importData(srcDataSource, dataSource, config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是destination
            for (MappingConfig configTmp : rdbMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (srcDataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = MongodbEtlService.importData(srcDataSource, dataSource, configTmp, params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    /**
     * 获取总数方法
     *
     * @param task 任务名, 对应配置名
     * @return 总数
     */
    @Override
    public Map<String, Object> count(String task) {
        MappingConfig config = rdbMapping.get(task);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + SyncUtil.getDbTableName(dbMapping);
        Connection conn = null;
        Map<String, Object> res = new LinkedHashMap<>();
        try {
            conn = dataSource.getConnection();
            Util.sqlRS(conn, sql, rs -> {
                try {
                    if (rs.next()) {
                        Long rowCount = rs.getLong("cnt");
                        res.put("count", rowCount);
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            });
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        res.put("targetTable", SyncUtil.getDbTableName(dbMapping));

        return res;
    }

    /**
     * 获取对应canal instance name 或 mq topic
     *
     * @param task 任务名, 对应配置名
     * @return destination
     */
    @Override
    public String getDestination(String task) {
        MappingConfig config = rdbMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    /**
     * 销毁方法
     */
    @Override
    public void destroy() {
        if (mongodbConfigMonitor != null) {
            mongodbConfigMonitor.destroy();
        }

        if (mongodbSyncService != null) {
            dataSource.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }
}
