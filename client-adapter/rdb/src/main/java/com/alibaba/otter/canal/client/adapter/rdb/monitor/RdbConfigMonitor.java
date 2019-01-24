package com.alibaba.otter.canal.client.adapter.rdb.monitor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.rdb.config.MirrorDbConfig;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;

public class RdbConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(RdbConfigMonitor.class);

    private static final String   adapterName = "rdb";

    private String                key;

    private RdbAdapter            rdbAdapter;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, RdbAdapter rdbAdapter) {
        this.key = key;
        this.rdbAdapter = rdbAdapter;
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
                if (rdbAdapter.getRdbMapping().containsKey(file.getName())) {
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
                        if (rdbAdapter.getRdbMapping().containsKey(file.getName())) {
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
                if (rdbAdapter.getRdbMapping().containsKey(file.getName())) {
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
            rdbAdapter.getRdbMapping().put(file.getName(), mappingConfig);
            if (!mappingConfig.getDbMapping().getMirrorDb()) {
                Map<String, MappingConfig> configMap = rdbAdapter.getMappingConfigCache()
                    .computeIfAbsent(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                                     + mappingConfig.getDbMapping().getDatabase() + "."
                                     + mappingConfig.getDbMapping().getTable(),
                        k1 -> new HashMap<>());
                configMap.put(file.getName(), mappingConfig);
            } else {
                Map<String, MirrorDbConfig> mirrorDbConfigCache = rdbAdapter.getMirrorDbConfigCache();
                mirrorDbConfigCache.put(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                                        + mappingConfig.getDbMapping().getDatabase(),
                    MirrorDbConfig.create(file.getName(), mappingConfig));
            }
        }

        private void deleteConfigFromCache(File file) {
            MappingConfig mappingConfig = rdbAdapter.getRdbMapping().remove(file.getName());

            if (mappingConfig == null || mappingConfig.getDbMapping() == null) {
                return;
            }
            if (!mappingConfig.getDbMapping().getMirrorDb()) {
                for (Map<String, MappingConfig> configMap : rdbAdapter.getMappingConfigCache().values()) {
                    if (configMap != null) {
                        configMap.remove(file.getName());
                    }
                }
            } else {
                rdbAdapter.getMirrorDbConfigCache().forEach((key, mirrorDbConfig) -> {
                    if (mirrorDbConfig.getFileName().equals(file.getName())) {
                        rdbAdapter.getMirrorDbConfigCache().remove(key);
                    }
                });
            }

        }
    }
}
