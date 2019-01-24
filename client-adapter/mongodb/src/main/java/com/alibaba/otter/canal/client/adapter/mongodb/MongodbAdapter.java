package com.alibaba.otter.canal.client.adapter.mongodb;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.mongodb.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MongodbTemplate;
import com.alibaba.otter.canal.client.adapter.mongodb.monitor.MongodbConfigMonitor;
import com.alibaba.otter.canal.client.adapter.mongodb.service.MongodbSyncService;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * mongodb
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

    private MongodbSyncService mongodbSyncService;
    private MongodbConfigMonitor mongodbConfigMonitor;

    private MongodbTemplate mongodbTemplate;


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
            logger.info("mappingConfig.getOuterAdapterKey():{}",mappingConfig.getOuterAdapterKey());
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

        try {
            /**
             *  初始化 mongodb  连接信息
             */
            mongodbTemplate = new MongodbTemplate(configuration);
        } catch (Exception e) {
            logger.error("ERROR ## failed to initial mongClient: {}", e);
        }
        mongodbSyncService = new MongodbSyncService(mongodbTemplate);
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
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        Future<Boolean> future1 = executorService.submit(() -> {
            if (!dmls.isEmpty()){
                dmls.forEach(dml -> {
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                    singleDmls.forEach(dm->{
                        sync(dm);

                    });
                });
            }
            return true;
        });
        try {
            future1.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public void sync(SingleDml dml) {
        if (dml == null) {
            return;
        }
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, MappingConfig> configMap = mappingConfigCache.get(destination + "." + database + "." + table);
        if (configMap != null) {
            configMap.values().forEach(config -> mongodbSyncService.sync(config, dml));
        }
    }

    /**
     * ETL方法
     *
     * @param task 任务名, 对应配置名
     * @param params etl筛选条件
     * @return ETL结果
     */
//    @Override
//    public EtlResult etl(String task, List<String> params) {
//        EtlResult etlResult = new EtlResult();
//        MappingConfig config = rdbMapping.get(task);
//        if (config != null) {
//            DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
//            if (srcDataSource != null) {
//                return MongodbEtlService.importData(srcDataSource, mongodbTemplate, config, params);
//            } else {
//                etlResult.setSucceeded(false);
//                etlResult.setErrorMessage("DataSource not found");
//                return etlResult;
//            }
//        } else {
//            StringBuilder resultMsg = new StringBuilder();
//            boolean resSucc = true;
//            // ds不为空说明传入的是destination
//            for (MappingConfig configTmp : rdbMapping.values()) {
//                // 取所有的destination为task的配置
//                if (configTmp.getDestination().equals(task)) {
//                    DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
//                    if (srcDataSource == null) {
//                        continue;
//                    }
//                    EtlResult etlRes = MongodbEtlService.importData(srcDataSource, mongodbTemplate, configTmp, params);
//                    if (!etlRes.getSucceeded()) {
//                        resSucc = false;
//                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
//                    } else {
//                        resultMsg.append(etlRes.getResultMessage()).append("\n");
//                    }
//                }
//            }
//            if (resultMsg.length() > 0) {
//                etlResult.setSucceeded(resSucc);
//                if (resSucc) {
//                    etlResult.setResultMessage(resultMsg.toString());
//                } else {
//                    etlResult.setErrorMessage(resultMsg.toString());
//                }
//                return etlResult;
//            }
//        }
//        etlResult.setSucceeded(false);
//        etlResult.setErrorMessage("Task not found");
//        return etlResult;
//    }

    /**
     * 获取总数方法
     *
     * @param task 任务名, 对应配置名
     * @return 总数
     */
//    @Override
//    public Map<String, Object> count(String task) {
//        MappingConfig config = rdbMapping.get(task);
//        MappingConfig.DbMapping dbMapping = config.getDbMapping();
//        //目标库
//        String dataBase = dbMapping.getTargetDb();
//        //目标表
//        String collection = dbMapping.getTargetTable();
//        MongoCollection<Document> collections;
//        Map<String, Object> res = new LinkedHashMap<>();
//        long count = 0L;
//        try {
//            //连接库信息
//            collections = mongodbTemplate.getCollection(dataBase,collection);
//            count = collections.count();
//            // 获取插入总数
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//        } finally {
//            if (mongodbTemplate.getMongoClient() != null) {
//                try {
//                    //关闭连接
//                    mongodbTemplate.close();
//                } catch (Exception e) {
//                    logger.error(e.getMessage(), e);
//                }
//            }
//        }
//        res.put("targetTable", SyncUtil.getDbTableName(dbMapping));
//        res.put("count",count);
//        return res;
//    }

    /**
     * 获取对应canal instance name 或 mq topic
     *
     * @param task 任务名, 对应配置名
     * @return destination
     */
//    @Override
//    public String getDestination(String task) {
//        MappingConfig config = rdbMapping.get(task);
//        if (config != null) {
//            return config.getDestination();
//        }
//        return null;
//    }

    /**
     * 销毁方法
     */
    @Override
    public void destroy() {
        if (mongodbConfigMonitor != null) {
            mongodbConfigMonitor.destroy();
        }

        if (mongodbSyncService != null) {
            mongodbTemplate.close();
        }

        if (mongodbTemplate != null) {
            mongodbTemplate.close();
        }
    }
}
