package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.rdb.config.MirrorDbConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.rdb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

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

    private DataSource dataSources;
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
        this.dataSources=dataSource;
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
            futures.clear();
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
            if (dml.getData() == null && StringUtils.isNotEmpty(dml.getSql())) {
                if(dml.getTable().equals("meta_history")||dml.getTable().equals("meta_snapshot")){
                    logger.info("================不同步binlog日志表=============");
                }else {
                    executeDdl(dml);
                }
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
                    //封装提取原始binlog的DML
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                    singleDmls.forEach(singleDml -> {
                        int hash = pkHash(config.getDbMapping(), singleDml.getData());
                        SyncItem syncItem = new SyncItem(config, singleDml);
                        dmlsPartition[hash].add(syncItem);
                    });
                    singleDmls.clear();
                } else {
                    int hash = 0;
                    //对  dml数据进行再封装
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                    singleDmls.forEach(singleDml -> {
                        SyncItem syncItem = new SyncItem(config, singleDml);
                        dmlsPartition[hash].add(syncItem);
                    });
                    singleDmls.clear();
                }
                executed = true;
            }
            return executed;
        }
    }   );
    }
    /**
     * DDL 操作
     *
     * @param ddl DDL
     */
    private void executeDdl(Dml ddl) {
        try (Connection conn = dataSources.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.getSql());
            statement.close();
            conn.close();
            if (logger.isTraceEnabled()) {
                logger.trace("Execute DDL sql: {} for database: {}", ddl.getSql(), ddl.getDatabase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        DbMapping dbMapping = config.getDbMapping();

        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");

        columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(targetColumnName).append(","));
        int len = insertSql.length();
        //删除循环列名产生的最后的逗号
        insertSql.delete(len - 1, len).append(") VALUES (");
        int mapLen = columnsMap.size();
        //根据列名的多少拼装?,
        for (int i = 0; i < mapLen; i++) {
            insertSql.append("?,");
        }
        len = insertSql.length();
        //删除多拼装的,
        insertSql.delete(len - 1, len).append(")");
        //获取目标字段类型   此方法可以保留作为mongodb的获取目标字段的方法
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        List<Map<String, ?>> values = new ArrayList<>();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            //dbMapping    目标字段名称
            String targetColumnName = entry.getKey();
            //dml.getData   源字段名称
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                //进行字符的替换  标点符号的替换
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            //通过对目标字段转换大写获取字段类型   目标字段类型值
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            //获取源字段对应数据
            Object value = data.get(srcColumnName);
            //绑定源数据对应的目标数据类型的int值
            BatchExecutor.setValue(values, type, value);
        }

        try {
            //同步
            batchExecutor.execute(insertSql.toString(), values);
        } catch (SQLException e) {
            if (skipDupException
                && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001: 违反唯一约束条件"))) {
                // ignore
                // TODO 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
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
        DbMapping dbMapping = config.getDbMapping();


        //目标表信息 目标表数据
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
        //获取目标字段类型   此方法可以保留作为mongodb的获取目标字段的方法
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder updateSql = new StringBuilder();
        updateSql.append("UPDATE ").append(SyncUtil.getDbTableName(dbMapping)).append(" SET ");
        List<Map<String, ?>> values = new ArrayList<>();
        //遍历旧数据的 源字段名
        for (String srcColumnName : old.keySet()) {
            //目标字段名
            List<String> targetColumnNames = new ArrayList<>();
            columnsMap.forEach((targetColumn, srcColumn) -> {
                if (srcColumnName.toLowerCase().equals(srcColumn)) {
                    targetColumnNames.add(targetColumn);
                }
            });
            if (!targetColumnNames.isEmpty()) {
                //遍历目标字段名
                for (String targetColumnName : targetColumnNames) {
                    //set 字段 = ?
                    updateSql.append(targetColumnName).append("=?, ");
                    //字段类型
                    Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                    if (type == null) {
                        throw new RuntimeException("Target column: " + targetColumnName + " not matched");
                    }
                    //绑定源数据对应的目标数据类型的int值
                    BatchExecutor.setValue(values, type, data.get(srcColumnName));
                }
            }
            targetColumnNames.clear();
        }
        int len = updateSql.length();
        // 去掉循环多余的字符  拼接sql
        updateSql.delete(len - 2, len).append(" WHERE ");

        // 拼接主键
        appendCondition(dbMapping, updateSql, ctype, values, data, old);
        batchExecutor.execute(updateSql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Update target table, sql: {}", updateSql);
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
        DbMapping dbMapping = config.getDbMapping();

        //获取目标字段类型   此方法可以保留作为mongodb的获取目标字段的方法
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(SyncUtil.getDbTableName(dbMapping)).append(" WHERE ");

        List<Map<String, ?>> values = new ArrayList<>();
        // 拼接主键
        appendCondition(dbMapping, sql, ctype, values, data);
        batchExecutor.execute(sql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Delete from target table, sql: {}", sql);
        }
    }

    /**
     * 获取目标字段类型
     *
     * @param conn sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        //获取mytest_user.yml的目标表配置信息
        //如果添加mongodb的数据同步的时候，可以针对此方法修改 ，同时可以自定义配置字段
        DbMapping dbMapping = config.getDbMapping();
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
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                            }
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
    public int pkHash(DbMapping dbMapping, Map<String, Object> d) {
        return pkHash(dbMapping, d, null);
    }

    public int pkHash(DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o) {
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
