package com.alibaba.otter.canal.client.adapter.mongodb.monitor;

import com.alibaba.otter.canal.client.adapter.mongodb.MongodbAdapter;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.mongodb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class MongodbConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(MongodbConfigMonitor.class);

    private static final String   adapterName = "mongod";

    private String                key;

    private MongodbAdapter mongodbAdapter;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, MongodbAdapter mongodbAdapter) {
        this.key = key;
        this.mongodbAdapter = mongodbAdapter;
        //1.获取resources下rdb文件路径
        File confDir = Util.getConfDirPath(adapterName);
        try {
            //2、对rdb下的yml文件进行监控变化
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                    FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            //3、为观察对象添加收听对象
            observer.addListener(listener);
            //4、配置Monitor，第一个参数单位是毫秒，是监听的间隔；第二个参数就是绑定我们之前的观察对象。
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileCreate(File file) {
            super.onFileCreate(file);
            try {
                // 加载新增的配置文件
                String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);
                config.validate();
                if ((key == null && config.getOuterAdapterKey() == null)
                    || (key != null && key.equals(config.getOuterAdapterKey()))) {
                    addConfigToCache(file, config);

                    logger.info("Add a new mongodb mapping config: {} to canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (mongodbAdapter.getRdbMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                        .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);
                    config.validate();
                    if ((key == null && config.getOuterAdapterKey() == null)
                        || (key != null && key.equals(config.getOuterAdapterKey()))) {
                        if (mongodbAdapter.getRdbMapping().containsKey(file.getName())) {
                            deleteConfigFromCache(file);
                        }
                        addConfigToCache(file, config);
                    } else {
                        // 不能修改outerAdapterKey
                        throw new RuntimeException("Outer adapter key not allowed modify");
                    }
                    logger.info("Change a mongodb mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (mongodbAdapter.getRdbMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a mongodb mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig mappingConfig) {
            if (mappingConfig == null || mappingConfig.getDbMapping() == null) {
                return;
            }
            mongodbAdapter.getRdbMapping().put(file.getName(), mappingConfig);
            if (!mappingConfig.getDbMapping().getMirrorDb()) {
                Map<String, MappingConfig> configMap = mongodbAdapter.getMappingConfigCache()
                    .computeIfAbsent(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                                     + mappingConfig.getDbMapping().getDatabase() + "."
                                     + mappingConfig.getDbMapping().getTable(),
                        k1 -> new HashMap<>());
                configMap.put(file.getName(), mappingConfig);
            } else {
                Map<String, MirrorDbConfig> mirrorDbConfigCache = mongodbAdapter.getMirrorDbConfigCache();
                mirrorDbConfigCache.put(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                                        + mappingConfig.getDbMapping().getDatabase(),
                    MirrorDbConfig.create(file.getName(), mappingConfig));
            }
        }

        private void deleteConfigFromCache(File file) {
            MappingConfig mappingConfig = mongodbAdapter.getRdbMapping().remove(file.getName());

            if (mappingConfig == null || mappingConfig.getDbMapping() == null) {
                return;
            }
            if (!mappingConfig.getDbMapping().getMirrorDb()) {
                for (Map<String, MappingConfig> configMap : mongodbAdapter.getMappingConfigCache().values()) {
                    if (configMap != null) {
                        configMap.remove(file.getName());
                    }
                }
            } else {
                mongodbAdapter.getMirrorDbConfigCache().forEach((key, mirrorDbConfig) -> {
                    if (mirrorDbConfig.getFileName().equals(file.getName())) {
                        mongodbAdapter.getMirrorDbConfigCache().remove(key);
                    }
                });
            }

        }
    }
}
