package com.alibaba.otter.canal.adapter.launcher.config;

import javax.annotation.PostConstruct;

import com.alibaba.otter.canal.adapter.launcher.monitor.AdapterRemoteConfigMonitor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

/**
 * Bootstrap级别配置加载
 *
 *
 * bootstrap级别的配置加载，优先于application.yml文件加载
 * appication.yml文件主要用于配置书写
 *
 * META-INF  存放程序的入口信息     canal的适配器程序启动后会优先进入此方法中
 *
 * @author rewerma @ 2019-01-05
 * @version 1.0.0
 */
public class BootstrapConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapConfiguration.class);

    @Autowired
    private Environment         env;

    @PostConstruct
    public void loadRemoteConfig() {
        try {
            // 加载远程配置
            String jdbcUrl = env.getProperty("canal.manager.jdbc.url");
            if (StringUtils.isNotEmpty(jdbcUrl)) {
                String jdbcUsername = env.getProperty("canal.manager.jdbc.username");
                String jdbcPassword = env.getProperty("canal.manager.jdbc.password");
                AdapterRemoteConfigMonitor configMonitor = new AdapterRemoteConfigMonitor(jdbcUrl,
                    jdbcUsername,
                    jdbcPassword);
                //加载远程application.yml配置文件
                configMonitor.loadRemoteConfig();
                //加载远程mytest_user1.yml文件
                configMonitor.loadRemoteAdapterConfigs();
                // 启动监听
                configMonitor.start();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
