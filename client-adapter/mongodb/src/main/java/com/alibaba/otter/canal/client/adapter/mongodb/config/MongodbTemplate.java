package com.alibaba.otter.canal.client.adapter.mongodb.config;

import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 *@Author cuitong
 *@Date: 2019/1/25 11:31
 *@Email: cuitong_sl@163.com
 *@Description:  mongodb数据源信息
 */
public class MongodbTemplate {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private MongoClient mongoClient;
    private OuterAdapterConfig configuration;
    private MongoCollection<Document> collections;
    /**
     * 读写分离
     */
    public MongodbTemplate(OuterAdapterConfig configuration){
        this.configuration= configuration;
    }
    private String readPreference = "PRIMARY";

    public MongoClient getMongoClient() {
        if (mongoClient == null){
            Map<String, String> properties = configuration.getProperties();
            String hostports = properties.get("mongo.core.replica.set");
            logger.info("hostports:{}",hostports);
            String username = properties.get("mongo.core.username");
            logger.info("username:{}",username);
            String password = properties.get("mongo.core.password");
            logger.info("password:{}",password);
            String database = properties.get("mongo.core.dbname");
            logger.info("database:{}",database);
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
                logger.error("【mongodb client】: mongodb客户端创建失败:{}",e);
                e.printStackTrace();
            }
            return mongoClient;
        }else {
            return mongoClient;
        }
    }
    /**
     *  连接  库
     */
    public MongoDatabase mongoDatabase(MongoClient mongoClient, String database) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        return mongoDatabase;
    }
    /**
     * @param dataBase 库
     * @param collection 集合
     */
    public MongoCollection<Document> getCollection(String dataBase,String collection){
            try {
                //选择mongo库
                MongoDatabase mongoDatabase = this.mongoDatabase(this.getMongoClient(),dataBase);
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
     * 关闭方法
     */
    public void close(){
        if (mongoClient!=null){
            mongoClient.close();
        }
    }
}
