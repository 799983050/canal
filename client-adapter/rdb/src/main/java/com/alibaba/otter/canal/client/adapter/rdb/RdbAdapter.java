package com.alibaba.otter.canal.client.adapter.rdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.rdb.logger.LoggerMessager;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.rdb.monitor.RdbConfigMonitor;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbEtlService;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbMirrorDbSyncService;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbSyncService;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * RDB适配器实现类
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
@SPI("rdb")
public class RdbAdapter implements OuterAdapter {

    private static Logger                           logger              = LoggerFactory.getLogger(RdbAdapter.class);

    private Map<String, MappingConfig>              rdbMapping          = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();                // 库名-表名对应配置
    private Map<String, MirrorDbConfig>             mirrorDbConfigCache = new ConcurrentHashMap<>();                // 镜像库配置

    private DruidDataSource                         dataSource;

    private RdbSyncService                          rdbSyncService;
    private RdbMirrorDbSyncService                  rdbMirrorDbSyncService;

    private RdbConfigMonitor                        rdbConfigMonitor;

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
        //加载配置文件
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
            throw new RuntimeException("No mongodb adapter found for config key: " + configuration.getKey());
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

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        // String commitSize = properties.get("commitSize");

        boolean skipDupException = BooleanUtils.toBoolean(configuration.getProperties()
            .getOrDefault("skipDupException", "true"));
        rdbSyncService = new RdbSyncService(dataSource,
            threads != null ? Integer.valueOf(threads) : null,
            skipDupException);

        rdbMirrorDbSyncService = new RdbMirrorDbSyncService(mirrorDbConfigCache,
            dataSource,
            threads != null ? Integer.valueOf(threads) : null,
            rdbSyncService.getColumnsTypeCache(),
            skipDupException);

        rdbConfigMonitor = new RdbConfigMonitor();
        rdbConfigMonitor.init(configuration.getKey(), this);
    }

    /**
     * 同步方法
     *
     * @param dmls 数据包
     */
    @Override
    public void sync(List<Dml> dmls) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        /**
         * 提供了两种选择：一种rdbSyncService：自己可以对字段做映射关系
         * 另一种rdbMirrorDbSyncService： 就是字段完全一样（镜像库）
         */
        long start = System.currentTimeMillis();
        LoggerMessager.batchSyncStart(start);
        Future<Boolean> future1 = executorService.submit(() -> {
            rdbSyncService.sync(mappingConfigCache, dmls);
            return true;
        });
        Future<Boolean> future2 = executorService.submit(() -> {
            rdbMirrorDbSyncService.sync(dmls);
            return true;
        });
        try {
            future1.get();
            future2.get();
            long over = System.currentTimeMillis();
            LoggerMessager.batchSyncOver(start,over);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            executorService.shutdown();
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
                return RdbEtlService.importData(srcDataSource, dataSource, config, params);
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
                    EtlResult etlRes = RdbEtlService.importData(srcDataSource, dataSource, configTmp, params);
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
        if (rdbConfigMonitor != null) {
            rdbConfigMonitor.destroy();
        }

        if (rdbSyncService != null) {
            rdbSyncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }
}
