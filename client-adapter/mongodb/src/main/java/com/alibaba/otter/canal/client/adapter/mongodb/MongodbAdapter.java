package com.alibaba.otter.canal.client.adapter.mongodb;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.mongodb.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MongodbTemplate;
import com.alibaba.otter.canal.client.adapter.mongodb.monitor.MongodbConfigMonitor;
import com.alibaba.otter.canal.client.adapter.mongodb.service.MongodbSyncService;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.support.*;
import com.mongodb.MongoClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 *@Author cuitong
 *@Date: 2019/1/25 11:30
 *@Email: cuitong_sl@163.com
 *@Description:  mongodb同步适配器
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
    private MongoClient mongoClient;

    private ExecutorService executorService = Executors.newFixedThreadPool(1);

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
            mongoClient = mongodbTemplate.getMongoClient();
        } catch (Exception e) {
            logger.error("ERROR ## failed to initial mongClient: {}", e);
        }
        mongodbSyncService = new MongodbSyncService(mongoClient,null,mongodbTemplate);
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
        Future<Boolean> future1 = executorService.submit(() -> {
            mongodbSyncService.batchSync(mappingConfigCache,dmls);
            return true;
        });
        try {
            future1.get();
        } catch (Exception e) {
            executorService.shutdown();
            throw new RuntimeException(e);
        }

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
            mongodbSyncService.close();
        }

        if (executorService!=null){
            executorService.shutdown();
        }
    }
}
