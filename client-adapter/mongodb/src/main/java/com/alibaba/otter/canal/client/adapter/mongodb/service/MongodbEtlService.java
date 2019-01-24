//package com.alibaba.otter.canal.client.adapter.mongodb.service;
//
//import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
//import com.alibaba.otter.canal.client.adapter.mongodb.config.MongodbTemplate;
//import com.alibaba.otter.canal.client.adapter.mongodb.support.SyncUtil;
//import com.alibaba.otter.canal.client.adapter.support.EtlResult;
//import com.alibaba.otter.canal.client.adapter.support.Util;
//import com.google.common.base.Joiner;
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.sql.DataSource;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * RDB ETL 操作业务类
// *
// * @author rewerma @ 2018-11-7
// * @version 1.0.0
// */
//public class MongodbEtlService {
//
//    private static final Logger logger = LoggerFactory.getLogger(MongodbEtlService.class);
//
//    /**
//     * 导入数据
//     */
//    public static EtlResult importData(DataSource srcDS, MongodbTemplate mongodbTemplate, MappingConfig config, List<String> params) {
//        EtlResult etlResult = new EtlResult();
//        //AtomicLong  创建具有初始值 0 的新 AtomicLong。
//        AtomicLong successCount = new AtomicLong();
//        //错误信息
//        List<String> errMsg = new ArrayList<>();
//        String hbaseTable = "";
//        try {
//            if (config == null) {
//                logger.error("Config is null!");
//                etlResult.setSucceeded(false);
//                etlResult.setErrorMessage("Config is null!");
//                return etlResult;
//            }
//            MappingConfig.DbMapping dbMapping = config.getDbMapping();
//
//            long start = System.currentTimeMillis();
//
//            // 拼接sql
//            StringBuilder sql = new StringBuilder("SELECT * FROM " + dbMapping.getDatabase() + "."
//                                                  + dbMapping.getTable());
//
//            // 拼接条件
//            appendCondition(params, dbMapping, srcDS, sql);
//
//            // 获取总数
//            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
//            long cnt = (Long) Util.sqlRS(srcDS, countSql, rs -> {
//                Long count = null;
//                try {
//                    if (rs.next()) {
//                        count = ((Number) rs.getObject(1)).longValue();
//                    }
//                } catch (Exception e) {
//                    logger.error(e.getMessage(), e);
//                }
//                return count == null ? 0 : count;
//            });
//
//            // 当大于1万条记录时开启多线程
//            if (cnt >= 10000) {
//                //三个线程
//                int threadCount = 3;
//                //每个线程处理的数量
//                long perThreadCnt = cnt / threadCount;
//                //创建定长线程池
//                ExecutorService executor = Executors.newFixedThreadPool(threadCount);
//                List<Future<Boolean>> futures = new ArrayList<>(threadCount);
//                for (int i = 0; i < threadCount; i++) {
//                    long offset = i * perThreadCnt;
//                    Long size = null;
//                    if (i != threadCount - 1) {
//                        size = perThreadCnt;
//                    }
//                    String sqlFinal;
//                    if (size != null) {
//                        sqlFinal = sql + " LIMIT " + offset + "," + size;
//                    } else {
//                        sqlFinal = sql + " LIMIT " + offset + "," + cnt;
//                    }
//                    Future<Boolean> future = executor.submit(() -> executeSqlImport(srcDS,
//                            mongodbTemplate,
//                        sqlFinal,
//                        dbMapping,
//                        successCount,
//                        errMsg));
//                    futures.add(future);
//                }
//
//                for (Future<Boolean> future : futures) {
//                    future.get();
//                }
//
//                executor.shutdown();
//            } else {
//                executeSqlImport(srcDS, mongodbTemplate, sql.toString(), dbMapping, successCount, errMsg);
//            }
//
//            logger.info(dbMapping.getTable() + " etl completed in: " + (System.currentTimeMillis() - start) / 1000
//                        + "s!");
//
//            etlResult.setResultMessage("导入目标表 " + SyncUtil.getDbTableName(dbMapping) + " 数据：" + successCount.get()
//                                       + " 条");
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//            errMsg.add(hbaseTable + " etl failed! ==>" + e.getMessage());
//        }
//
//        if (errMsg.isEmpty()) {
//            etlResult.setSucceeded(true);
//        } else {
//            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
//        }
//        return etlResult;
//    }
//
//    private static void appendCondition(List<String> params, MappingConfig.DbMapping dbMapping, DataSource ds, StringBuilder sql)
//                                                                                                                   throws SQLException {
//        if (params != null && params.size() == 1 && dbMapping.getEtlCondition() == null) {
//            /**
//             *public AtomicBoolean(boolean initialValue) {
//             *         value = initialValue ? 1 : 0;
//             *     }
//             */
//            AtomicBoolean stExists = new AtomicBoolean(false);
//            // 验证是否有SYS_TIME字段
//            Util.sqlRS(ds, sql.toString(), rs -> {
//                try {
//                    ResultSetMetaData rsmd = rs.getMetaData();
//                    int cnt = rsmd.getColumnCount();
//                    for (int i = 1; i <= cnt; i++) {
//                        String columnName = rsmd.getColumnName(i);
//                        if ("SYS_TIME".equalsIgnoreCase(columnName)) {
//                            stExists.set(true);
//                            break;
//                        }
//                    }
//                } catch (Exception e) {
//                    // ignore
//            }
//            return null;
//        }   );
//            if (stExists.get()) {
//                sql.append(" WHERE SYS_TIME >= '").append(params.get(0)).append("' ");
//            }
//        } else if (dbMapping.getEtlCondition() != null && params != null) {
//            String etlCondition = dbMapping.getEtlCondition();
//            int size = params.size();
//            for (int i = 0; i < size; i++) {
//                etlCondition = etlCondition.replace("{" + i + "}", params.get(i));
//            }
//
//            sql.append(" ").append(etlCondition);
//        }
//    }
//
//    /**
//     * 执行导入
//     */
//    private static boolean executeSqlImport(DataSource srcDS, MongodbTemplate mongodbTemplate, String sql, MappingConfig.DbMapping dbMapping,
//                                            AtomicLong successCount, List<String> errMsg) {
//        try {
//            Util.sqlRS(srcDS, sql, rs -> {
//                int idx = 1;
//
//                try {
//                    boolean completed = false;
//
//                    Map<String, Integer> columnType = new LinkedHashMap<>();
//                    //获取数据
//                    ResultSetMetaData rsd = rs.getMetaData();
//                    //获取列数量
//                    int columnCount = rsd.getColumnCount();
//                    List<String> columns = new ArrayList<>();
//                    for (int i = 1; i <= columnCount; i++) {
//                        //类型和列名对应
//                        columnType.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
//                        columns.add(rsd.getColumnName(i));
//                    }
//
//                    Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, columns);
//                    // if (dbMapping.isMapAll()) {
//                    // columnsMap = dbMapping.getAllColumns();
//                    // } else {
//                    // columnsMap = dbMapping.getTargetColumns();
//                    // }
//
//
//                    Document document = new Document();
//
//
//
//
//                    StringBuilder insertSql = new StringBuilder();
//
//
//                insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");
//                columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(targetColumnName).append(","));
//
//                int len = insertSql.length();
//                insertSql.delete(len - 1, len).append(") VALUES (");
//                int mapLen = columnsMap.size();
//                for (int i = 0; i < mapLen; i++) {
//                    insertSql.append("?,");
//                }
//                len = insertSql.length();
//                insertSql.delete(len - 1, len).append(")");
//                try (Connection connTarget = targetDS.getConnection();
//                        PreparedStatement pstmt = connTarget.prepareStatement(insertSql.toString())) {
//                    connTarget.setAutoCommit(false);
//
//                    while (rs.next()) {
//                        pstmt.clearParameters();
//
//                        // 删除数据
//                        Map<String, Object> values = new LinkedHashMap<>();
//                        StringBuilder deleteSql = new StringBuilder("DELETE FROM " + SyncUtil.getDbTableName(dbMapping)
//                                                                    + " WHERE ");
//                        appendCondition(dbMapping, deleteSql, values, rs);
//                        try (PreparedStatement pstmt2 = connTarget.prepareStatement(deleteSql.toString())) {
//                            int k = 1;
//                            for (Object val : values.values()) {
//                                pstmt2.setObject(k++, val);
//                            }
//                            pstmt2.execute();
//                        }
//
//                        int i = 1;
//                        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
//                            String targetClolumnName = entry.getKey();
//                            String srcColumnName = entry.getValue();
//                            if (srcColumnName == null) {
//                                srcColumnName = targetClolumnName;
//                            }
//
//                            Integer type = columnType.get(targetClolumnName.toLowerCase());
//
//                            Object value = rs.getObject(srcColumnName);
//                            if (value != null) {
//                                SyncUtil.setPStmt(type, pstmt, value, i);
//                            } else {
//                                pstmt.setNull(i, type);
//                            }
//
//                            i++;
//                        }
//
//                        pstmt.execute();
//                        if (logger.isTraceEnabled()) {
//                            logger.trace("Insert into target table, sql: {}", insertSql);
//                        }
//
//                        if (idx % dbMapping.getCommitBatch() == 0) {
//                            connTarget.commit();
//                            completed = true;
//                        }
//                        idx++;
//                        successCount.incrementAndGet();
//                        if (logger.isDebugEnabled()) {
//                            logger.debug("successful import count:" + successCount.get());
//                        }
//                    }
//                    if (!completed) {
//                        connTarget.commit();
//                    }
//                }
//
//            } catch (Exception e) {
//                logger.error(dbMapping.getTable() + " etl failed! ==>" + e.getMessage(), e);
//                errMsg.add(dbMapping.getTable() + " etl failed! ==>" + e.getMessage());
//            }
//            return idx;
//        }   );
//            return true;
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//            return false;
//        }
//    }
//
//    /**
//     * 拼接目标表主键where条件
//     */
//    private static void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Object> values, ResultSet rs)
//                                                                                                                         throws SQLException {
//        // 拼接主键
//        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
//            String targetColumnName = entry.getKey();
//            String srcColumnName = entry.getValue();
//            if (srcColumnName == null) {
//                srcColumnName = targetColumnName;
//            }
//            sql.append(targetColumnName).append("=? AND ");
//            values.put(targetColumnName, rs.getObject(srcColumnName));
//        }
//        int len = sql.length();
//        sql.delete(len - 4, len);
//    }
//}
