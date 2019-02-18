package com.alibaba.otter.canal.client.adapter.mongodb.logger;

import com.alibaba.otter.canal.client.adapter.mongodb.service.MongodbSyncService;
import com.alibaba.otter.canal.client.adapter.mongodb.support.SingleDml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@Author cuitong
 *@Date: 2019/1/31 15:42
 *@Email: cuitong_sl@163.com
 *@Description: 日志
 */
public class LoggerMessager {
    private static final Logger logger  = LoggerFactory.getLogger(LoggerMessager.class);

    /**
     *  获取的批数量大小  以及获取时间
     */
    public static void batchSyncStart(long batchStart){
        logger.info("批量同步开始时间 =====>>>> BatchSizeStart ----> this time is : [{}]",batchStart);
    }

    public static void singleSyncStart(long singleStartSync,String type,SingleDml dml){
        logger.info("单条同步开始时间 =====>>>> SingleDmlSync start ---> this time is :[{}],this type is :[{}]",singleStartSync,type);
        logger.info("同步数据展示 =====>>>> SingleDml ---> this data is :[{}]",dml.getData().entrySet());
    }

    //  单条同步结束时间和 业务处理时间
    public static void exceptionData(SingleDml dml){
        logger.info("同步失败数据: filed syncData =====>>>>:[{}]",dml);
    }

    public static void batchSyncOver(long batchStart,long batchOver){
        logger.info("批量同步 : 业务结束时间 =====>>>> 业务耗时 ：BatchSyncOver ----> this time is : [{}],The business time:[{}]",batchOver,batchOver-batchStart);
    }

    /**
     *  数据同步量实时监控
     */
    public static void DataSize(String targetTable,Integer allSize,Integer singleSize,Integer surplusSize){
        logger.info("数据同步量实时监控 : =====>>>> DataMessage ---> targetTable is :[{}],Total data :[{}], this time syncDataIndex :[{}] , this time surplusData :[{}]",targetTable,allSize,singleSize,surplusSize);
    }
}
