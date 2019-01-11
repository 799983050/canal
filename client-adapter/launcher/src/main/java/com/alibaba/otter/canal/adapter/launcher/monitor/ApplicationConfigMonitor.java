package com.alibaba.otter.canal.adapter.launcher.monitor;

import java.io.File;
import java.io.FileReader;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.adapter.launcher.loader.CanalAdapterService;
import com.alibaba.otter.canal.client.adapter.support.Util;
/**
 *   对application.yml文件的监听，文件改变则重新进行适配器的初始化
 */
@Component
public class ApplicationConfigMonitor {

    private static final Logger   logger = LoggerFactory.getLogger(ApplicationConfigMonitor.class);

    @Resource
    private ContextRefresher      contextRefresher;

    @Resource
    private CanalAdapterService   canalAdapterService;

    private FileAlterationMonitor fileMonitor;

    @PostConstruct
    public void init() {
        File confDir = Util.getConfDirPath();
        try {
            //对application.yml文件重写的一个监听配置
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                FileFilterUtils.and(FileFilterUtils.fileFileFilter(),
                    FileFilterUtils.prefixFileFilter("application"),
                    FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            //为监察对象添加收听对象
            observer.addListener(listener);
            //配置Monitor，第一个参数单位是毫秒，是监听的间隔；第二个参数就是绑定我们之前的观察对象。
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);
            try {
                // 检查yml格式
                new Yaml().loadAs(new FileReader(file), Map.class);
                //关闭适配器的连接
                canalAdapterService.destroy();

                // refresh context   springboot的热更新
                contextRefresher.refresh();

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // ignore
                }
                //开启适配器的连接
                canalAdapterService.init();
                logger.info("## adapter application config reloaded.");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
