package com.alibaba.otter.canal.client.adapter.mongodb.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.mongodb.MongodbAdapter;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MongodbTemplate;
import com.alibaba.otter.canal.client.adapter.mongodb.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.mongodb.MongoClientException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MongodbSyncService {

    private static final Logger               logger  = LoggerFactory.getLogger(MongodbSyncService.class);

    private MongodbTemplate mongodbTemplate;
    public MongodbSyncService(MongodbTemplate mongodbTemplate){
        this.mongodbTemplate = mongodbTemplate;
    }


    /**
     * 单条 dml 同步
     *
     * @param
     * @param config 对应配置对象
     * @param dml DML
     */
    public void sync(MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(config, dml);
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
    private void insert(MappingConfig config, SingleDml dml){
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
            Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
            //获取源数据字段类型
        document = new Document();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                //dml.getData   源字段名称
                String srcColumnName = entry.getValue();
                logger.info("源字段名:{}",srcColumnName);
                //获取源字段对应数据
                Object value = data.get(srcColumnName);
                logger.info("源字段数据:{}",value);
                document.put(srcColumnName,value);
            }
            logger.info("document.getUsername:{}",document.get("username"));
            logger.info("document.getPassword:{}",document.get("password"));


            /**
             * 向mongodb做缓存同步时不需要了解对应的类型，直接存储就行
             */
            try {
                String targetTable = dbMapping.getTargetTable();
                String[] split = targetTable.split("\\.");
                String database = split[0];
                String collection = split[1];
                logger.info("database:{}",database);
                logger.info("collection:{}",collection);
                //collection   可以对mongo库进行操作 插入数据
                collections = mongodbTemplate.getCollection(database,collection);
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
    private void update( MappingConfig config, SingleDml dml) {
        //获取数据列表
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        //获取旧数据列表
        //获取数据列表
        Map<String, Object> old = dml.getOld();
        if (old == null || old.isEmpty()) {
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
            logger.info("database:{}",database);
            logger.info("collection:{}",collection);
            //collection   可以对mongo库进行操作 插入数据
            collections = mongodbTemplate.getCollection(database,collection);
            documentNew = new Document();
            //遍历配置主键
            //获取主键
            String pk = null;
            //主键映射,获取主键
            Map<String, String> targetPk = dbMapping.getTargetPk();
            for (Map.Entry<String, String> entry : targetPk.entrySet()) {
                pk = entry.getValue();
            }
            logger.info("当前的主键为:{}",pk);
            Object pkValue = null;
            //遍历旧数据 获取pkData and pkName
                for (Map.Entry<String, Object> s : old.entrySet()) {
                    if (pk.equals(s.getKey())){
                        pkValue = s.getValue();
                    }else {
                        continue;
                    }
                }
                logger.info("当前的主键值为:{}",pkValue);
                //遍历新数据 获取pkData and pkName
                for (Map.Entry<String, Object> s : data.entrySet()) {
                    documentNew.put(s.getKey(), s.getValue());
                }
                //修改数据   获取_id   _id 和主键没有关系 通过mysql主键
                UpdateResult updateResult = collections.updateMany(Filters.eq(pk, pkValue), new Document("$set", documentNew));
                logger.info("更新的条数为:{},更新的id是:{}", updateResult.getMatchedCount(), updateResult.getUpsertedId());
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
    private void delete( MappingConfig config, SingleDml dml) throws SQLException {
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
            logger.info("database:{}",database);
            logger.info("collection:{}",collection);
            //collection   可以对mongo库进行操作 插入数据
            collections = mongodbTemplate.getCollection(database,collection);
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

}
