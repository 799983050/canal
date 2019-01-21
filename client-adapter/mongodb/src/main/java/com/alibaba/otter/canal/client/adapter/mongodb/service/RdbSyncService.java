package com.alibaba.otter.canal.client.adapter.mongodb.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.mongodb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.mongodb.MongoClientException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * RDB同步操作业务
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
public class RdbSyncService {

    private static final Logger               logger  = LoggerFactory.getLogger(RdbSyncService.class);

    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache;

    private int                               threads = 3;
    private boolean                           skipDupException;

    private List<SyncItem>[]                  dmlsPartition;
    private BatchExecutor[]                   batchExecutors;
    private ExecutorService[]                 executorThreads;

    public List<SyncItem>[] getDmlsPartition() {
        return dmlsPartition;
    }

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    @SuppressWarnings("unchecked")
    public RdbSyncService(DataSource dataSource, Integer threads, boolean skipDupException){
        this(dataSource, threads, new ConcurrentHashMap<>(), skipDupException);
    }

    @SuppressWarnings("unchecked")
    public RdbSyncService(DataSource dataSource, Integer threads, Map<String, Map<String, Integer>> columnsTypeCache,
                          boolean skipDupException){
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(dataSource.getConnection());
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
                        dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                            syncItem.config,
                            syncItem.singleDml));
                        dmlsPartition[j].clear();
                        batchExecutors[j].commit();
                        return true;
                    } catch (Throwable e) {
                        batchExecutors[j].rollback();
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
     *
     * @param mappingConfig 配置集合
     * @param dmls 批量 DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls) {
        sync(dmls, dml -> {
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
            columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
            return false;
        } else {
            // DML
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String database = dml.getDatabase();
            String table = dml.getTable();
            Map<String, MappingConfig> configMap = mappingConfig.get(destination + "." + database + "." + table);

            if (configMap == null) {
                return false;
            }

            boolean executed = false;
            for (MappingConfig config : configMap.values()) {
                if (config.getConcurrent()) {
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                    singleDmls.forEach(singleDml -> {
                        int hash = pkHash(config.getDbMapping(), singleDml.getData());
                        SyncItem syncItem = new SyncItem(config, singleDml);
                        dmlsPartition[hash].add(syncItem);
                    });
                } else {
                    int hash = 0;
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                    singleDmls.forEach(singleDml -> {
                        SyncItem syncItem = new SyncItem(config, singleDml);
                        dmlsPartition[hash].add(syncItem);
                    });
                }
                executed = true;
            }
            return executed;
        }
    }   );
    }

    /**
     * 单条 dml 同步
     *
     * @param batchExecutor 批量事务执行器
     * @param config 对应配置对象
     * @param dml DML
     */
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                }
            } catch (SQLException e) {
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
    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //缓存 类型
        getTargetColumnType(batchExecutor.getConn(), config);
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
        //获取源数据字段类型
        Document document = new Document();
        List<Document> values = new ArrayList<>();
        MongoCollection<Document> collections = null;
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            //dml.getData   源字段名称
            String srcColumnName = entry.getValue();
            //获取源字段对应数据
            Object value = data.get(srcColumnName);
            document.put(srcColumnName,value);
        }
        values.add(document);
        /**
         * 向mongodb做缓存同步时不需要了解对应的类型，直接存储就行
         */
        try {
            //collection   可以对mongo库进行操作 插入数据
            collections = getCollection(dbMapping.getTargetDb(),dbMapping.getTargetTable());
            collections.insertMany(values);
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
    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        //获取数据列表
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        //获取旧数据列表
        Map<String, Object> old = dml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }
        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //缓存 类型
        getTargetColumnType(batchExecutor.getConn(), config);
        //获取mongodn目标表数据
        MongoCollection<Document> collections = null;
        Document documentNew =null;

        try {
            collections = getCollection(dbMapping.getTargetDb(),dbMapping.getTargetTable());
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
            //遍历旧数据 获取pkData and pkName
            for (Map.Entry<String, Object> s : old.entrySet()) {
                if (pk.equals(s.getKey())){
                    pkValue = s.getValue();
                }else {
                    continue;
                }
            }
            //遍历新数据 获取pkData and pkName
            for (Map.Entry<String, Object> s : data.entrySet()) {
                documentNew.put(s.getKey(),s.getValue());
            }
            //修改数据   获取_id   _id 和主键没有关系 通过mysql主键
            UpdateResult updateResult = collections.updateMany(Filters.eq(pk, pkValue),new Document("$set", documentNew));
            logger.info("更新的条数为:{},更新的id是:{}",updateResult.getMatchedCount(),updateResult.getUpsertedId());
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
    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        //获取数据列表
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //缓存 类型
        getTargetColumnType(batchExecutor.getConn(), config);
        //直接删除
        //获取mongodn目标表数据
        MongoCollection<Document> collections = null;

        try {
            collections = getCollection(dbMapping.getTargetDb(),dbMapping.getTargetTable());
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
            DeleteResult deleteResult = collections.deleteMany(Filters.eq(pk, pkValue));
            logger.info("删除的条数为:{}",deleteResult.getDeletedCount());
        } catch (Exception e) {
            logger.info("mongodb数据删除失败:{},e");
        }
    }

    /**
     * 数据类型缓存
     *
     * @param conn sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        //获取字段类型缓存的数据
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        //字段缓存为空时，为缓存赋值
        if (columnType == null) {
            synchronized (RdbSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                //缓存为空
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;

                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                                //总字段数量
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                  //绑定字段与类型
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                            }
                              //缓存存入
                            columnsTypeCache.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                }
            }
        }
        return columnType;
    }
    /**
     * 连接mongodb的客户端
     */
    public MongoCollection<Document> getCollection(String dataBase,String collection){
        RdbAdapter rdbAdapter =null;
        MongoCollection<Document> collections = null;
        try {
            rdbAdapter = new RdbAdapter();
            //选择mongo库
            MongoDatabase mongoDatabase = rdbAdapter.mongoDatabase(rdbAdapter.getMongoClient(),dataBase);
            //选择表  连接目标表
            collections = mongoDatabase.getCollection(collection);
        }catch (MongoClientException | MongoSocketException clientException){
            // 客户端连接异常抛出，阻塞同步，防止mongodb宕机
            throw clientException;
        }catch (Exception e){
            logger.info("mongoClient collection error Exception:{}",e);
        }
        return collections;
    }
    /**
     * 拼接主键 where条件
     */
    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d) {
        appendCondition(dbMapping, sql, ctype, values, d, null);
    }

    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d, Map<String, Object> o) {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            // 获取目标主键字段名
            String targetColumnName = entry.getKey();
            // 获取源主键字段名
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                // 如果源主键字段名为空  将目标主键字段名赋给源主键
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            //
            sql.append(targetColumnName).append("=? AND ");
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            // 如果有修改主键的情况
            if (o != null && o.containsKey(srcColumnName)) {
                BatchExecutor.setValue(values, type, o.get(srcColumnName));
            } else {
                BatchExecutor.setValue(values, type, d.get(srcColumnName));
            }
        }
        int len = sql.length();
        sql.delete(len - 4, len);
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
