package com.alibaba.otter.canal.client.adapter.mongodb.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MongodbTemplate;
import com.alibaba.otter.canal.client.adapter.mongodb.logger.LoggerMessager;
import com.alibaba.otter.canal.client.adapter.mongodb.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 *@Author cuitong
 *@Date: 2019/1/25 11:30
 *@Email: cuitong_sl@163.com
 *@Description: mongodb同步业务
 */
public class MongodbSyncService {

    private static final Logger               logger  = LoggerFactory.getLogger(MongodbSyncService.class);
    private int                               threads = 3;
    private ExecutorService[]                 executorThreads;
    private List<SyncItem>[]                  dmlsPartition;
    private BatchExecutor[]                   batchExecutors;
    private MongodbTemplate mongodbTemplate;
    public MongodbSyncService(Integer threads,MongodbTemplate mongodbTemplate){
        this.mongodbTemplate = mongodbTemplate;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(mongodbTemplate.getMongoClient());
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    /**
     * 批量同步回调
     *
     * @param dmls 批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        boolean toExecute = false;
        for (Dml dml : dmls) {
            if (!toExecute) {
                //函数 获取对应的value
                toExecute = function.apply(dml);
            } else {
                function.apply(dml);
            }
        }
        if (toExecute) {
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int j = i;
                futures.add(executorThreads[i].submit(() -> {
                    try {
                        //创建单例线程池的作用是为了 以下方法串行执行
                        dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                                syncItem.config,
                                syncItem.singleDml));
                        dmlsPartition[j].clear();
                        return true;
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            futures.forEach(future -> {
                try {
                    future.get();
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * 批量同步
     */
    public void batchSync(Map<String, Map<String, MappingConfig>> mappingConfigCache,List<Dml> dmls){
        AtomicInteger count = new AtomicInteger(1);
        sync(dmls, dml -> {
            int counts = count.getAndIncrement();
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String database = dml.getDatabase();
            String table = dml.getTable();
            Map<String, MappingConfig> configMap = mappingConfigCache.get(destination + "." + database + "." + table);
            if (dml.getData() == null && StringUtils.isNotEmpty(dml.getSql())) {
                        // DDL
                 if (logger.isDebugEnabled()) {
                     logger.debug("DDL: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                  }
                 if (configMap!=null){
                     for (MappingConfig config : configMap.values()) {
                         executeDdl(dml,config);
                     }
                 }
                return false;
            }else {
                // DML
                if (configMap == null) {
                    return false;
                }
                boolean executed = false;
                for (MappingConfig config : configMap.values()) {
                    //判断配置文件是否开启缓存
                    if (config.getNoCache()){
                        continue;
                    }else {
                        if (config.getConcurrent()) {
                            //封装提取原始binlog的DML
                            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                            for (int i = 0; i < singleDmls.size(); i++) {
                                // 哪张表 数据同步总量  当前第几条  剩余多少条
                                LoggerMessager.DataSize(config.getDbMapping().getTargetTable(),dmls.size(),counts,dmls.size()-counts);
                                SingleDml singleDml = singleDmls.get(i);
                                int hash = pkHash(config.getDbMapping(), singleDml.getData());
                                SyncItem syncItem = new SyncItem(config, singleDml);
                                dmlsPartition[hash].add(syncItem);
                            }
                            singleDmls.clear();
                        } else {
                            int hash = 0;
                            //对  dml数据进行再封装
                            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                            for (int i = 0; i < singleDmls.size(); i++) {
                                //  数据同步总量  当前第几条  剩余多少条
                                LoggerMessager.DataSize(config.getDbMapping().getTargetTable(),dmls.size(),counts,dmls.size()-counts);
                                SingleDml singleDml = singleDmls.get(i);
                                SyncItem syncItem = new SyncItem(config, singleDml);
                                dmlsPartition[hash].add(syncItem);
                            }
                            singleDmls.clear();
                        }
                    }
                    executed = true;
                }
                return executed;
            }
        });
    }
    /**
     * DDL 操作
     *
     * @param ddl DDL
     */
    private void executeDdl(Dml ddl,MappingConfig config) {
        try {
            Map<String, String> columnsMap =null;
            if (config.getDbMapping().getAllMapColumns()!=null){
                logger.info("=============开始清除列名缓存============");
                config.getDbMapping().setAllMapColumns(columnsMap);
                logger.info("=============清除列名缓存结束============");
            }
        }catch (Exception e){
            logger.info("=============清除列名缓存异常============");
            throw new RuntimeException(e);
        }
        logger.trace("Execute DDL sql: {}, for database: {}", ddl.getSql(), ddl.getDatabase());
    }
    /**
     * 单条 dml 同步
     *
     * @param
     * @param config 对应配置对象
     * @param dml DML
     */
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor,config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor,config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor,config, dml);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                }
            } catch (SQLException e) {
                LoggerMessager.exceptionData(dml);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(BatchExecutor batchExecutor,MappingConfig config, SingleDml dml){
        //获取数据列表
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        Document document = null;
        MongoCollection<Document> collections = null;
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //获取不缓存的字段值
        List<String> notCache= new ArrayList<>();
        if (dbMapping.getTargetColumns()!=null){
            Map<String, String> notColumns = dbMapping.getTargetColumns();
            for (String s : notColumns.keySet()) {
                notCache.add(s);
            }
        }
        //获取主键
        String pk = null;
        //主键映射,获取主键
        Map<String, String> targetPk = dbMapping.getTargetPk();
        for (Map.Entry<String, String> entry : targetPk.entrySet()) {
            pk = entry.getValue();
        }

        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
            //获取源数据字段类型
        document = new Document();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            //如果额外配置不为空
            if (!notCache.isEmpty()){
                for (String s : notCache) {
                    if (entry.getValue().equals(s)){
                        continue;
                    }else {
                        //dml.getData   源字段名称
                        String srcColumnName = entry.getValue();
                        //获取源字段对应数据
                        Object value = data.get(srcColumnName);
                        if(srcColumnName.equals(pk)){
                            document.put("_id",value);
                        }else {
                            document.put(srcColumnName,value);
                        }
                    }
                }
            }else{
                //dml.getData   源字段名称
                String srcColumnName = entry.getValue();
                //获取源字段对应数据
                Object value = data.get(srcColumnName);
                if(srcColumnName.equals(pk)){
                    document.put("_id",value);
                }else {
                    document.put(srcColumnName,value);
                }
            }

            }

            /**
             * 向mongodb做缓存同步时不需要了解对应的类型，直接存储就行
             */
            try {
                String targetTable = dbMapping.getTargetTable();
                String[] split = targetTable.split("\\.");
                String database = split[0];
                String collection = split[1];
                //collection   可以对mongo库进行操作 插入数据
                collections = mongodbTemplate.getCollection(batchExecutor.getMongoClient(),database,collection);
                collections.insertOne(document);
            }catch (Exception e){
                logger.info("数据插入失败:{}",e);
            }
        }
    /**
     * 更新操作
     *
     * @param config 配置项
     * @param dml DML数据
     *
     *
     *    update  table   set  targetColumnName = ?  WHERE  targetColumnName = ?;
     */
    private void update( BatchExecutor batchExecutor,MappingConfig config, SingleDml dml) {
        //获取数据列表
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //获取mongodn目标表数据
        MongoCollection<Document> collections = null;
        Document documentNew =null;

        try {
            String targetTable = dbMapping.getTargetTable();
            String[] split = targetTable.split("\\.");
            String database = split[0];
            String collection = split[1];
            //collection   可以对mongo库进行操作 插入数据
            collections = mongodbTemplate.getCollection(batchExecutor.getMongoClient(),database,collection);
            documentNew = new Document();
            //遍历配置主键
            //获取主键
            String pk = null;
            //主键映射,获取主键
            Map<String, String> targetPk = dbMapping.getTargetPk();
            for (Map.Entry<String, String> entry : targetPk.entrySet()) {
                pk = entry.getValue();
            }
            Object pkValue = null;
            Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
            for (Map.Entry<String, String> mapping : columnsMap.entrySet()) {
                if (pk.equals(mapping.getValue())){
                    String pkName = mapping.getValue();
                    pkValue = data.get(pkName);
                }else {
                    continue;
                }
            }
            //获取不缓存的字段值
            List<String> notCache= new ArrayList<>();
            if (dbMapping.getTargetColumns()!=null){
                Map<String, String> notColumns = dbMapping.getTargetColumns();
                for (String s : notColumns.keySet()) {
                    notCache.add(s);
                }
            }
                //遍历新数据 获取pkData and pkName
                for (Map.Entry<String, Object> s : data.entrySet()) {
                    if (!notCache.isEmpty()){
                        for (String s1 : notCache) {
                            if (s.getKey().equals(s1)){
                                continue;
                            }else {
                                if(s.getKey().equals(pk)){
                                    documentNew.put("_id", s.getValue());
                                }else {
                                    documentNew.put(s.getKey(), s.getValue());
                                }
                            }
                        }

                    }else {
                        if(s.getKey().equals(pk)){
                            documentNew.put("_id", s.getValue());
                        }else {
                            documentNew.put(s.getKey(), s.getValue());
                        }
                    }
                }
                //修改数据   获取_id   _id 和主键没有关系 通过mysql主键
                UpdateResult updateResult = collections.updateMany(Filters.eq("_id", pkValue), new Document("$set", documentNew));
        } catch (Exception e) {
            logger.info("数据更新失败:{}",e);
        }
    }

    /**
     * 删除操作
     *
     * @param config
     * @param dml
     */
    private void delete( BatchExecutor batchExecutor,MappingConfig config, SingleDml dml) throws SQLException {
        //获取数据列表
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //直接删除
        //获取mongodn目标表数据
        MongoCollection<Document> collections = null;

        try {
            String targetTable = dbMapping.getTargetTable();
            String[] split = targetTable.split("\\.");
            String database = split[0];
            String collection = split[1];
            //collection   可以对mongo库进行操作 插入数据
            collections = mongodbTemplate.getCollection(batchExecutor.getMongoClient(),database,collection);
            //遍历配置主键
            //获取主键
            String pk = null;
            //主键映射,获取主键
            Map<String, String> targetPk = dbMapping.getTargetPk();
            for (Map.Entry<String, String> entry : targetPk.entrySet()) {
                pk = entry.getValue();
            }
            Object pkValue = null;
            //遍历旧数据 获取pkData and pkName
                for (Map.Entry<String, Object> s : data.entrySet()) {
                    if (pk.equals(s.getKey())){
                        pkValue = s.getValue();
                    }else {
                        continue;
                    }
                }
            DeleteResult deleteResult = collections.deleteMany(Filters.eq("_id", pkValue));
        } catch (Exception e) {
            logger.info("mongodb数据删除失败:{}",e);
        }
    }
    public static class SyncItem {

        private MappingConfig config;
        private SingleDml     singleDml;

        public SyncItem(MappingConfig config, SingleDml singleDml){
            this.config = config;
            this.singleDml = singleDml;
        }
    }

    /**
     * 取主键hash
     */
    public int pkHash(MappingConfig.DbMapping dbMapping, Map<String, Object> d) {
        return pkHash(dbMapping, d, null);
    }

    public int pkHash(MappingConfig.DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            Object value;
            if (o != null && o.containsKey(srcColumnName)) {
                value = o.get(srcColumnName);
            } else {
                value = d.get(srcColumnName);
            }
            if (value != null) {
                hash += value.hashCode();
            }
        }
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }
    public void close() {
        for (int i = 0; i < threads; i++) {
            batchExecutors[i].close();
            executorThreads[i].shutdown();
        }
    }
}
