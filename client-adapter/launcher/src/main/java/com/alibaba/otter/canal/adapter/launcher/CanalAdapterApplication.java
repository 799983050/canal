package com.alibaba.otter.canal.adapter.launcher;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 启动入口
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */

/**
 * @SpringBootApplication = @Configuration + @EnableAutoConfiguration + @ComponentScan。
 * 参考：https://www.cnblogs.com/MaxElephant/p/8108140.html
 */
@SpringBootApplication
public class CanalAdapterApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(CanalAdapterApplication.class);
        /*
         * 控制Banner的输出方式：
         * Banner.Mode.OFF:关闭;
         * Banner.Mode.CONSOLE:控制台输出，默认方式;
         * Banner.Mode.LOG:日志输出方式;
         * 参考：https://blog.csdn.net/linxingliang/article/details/52077802
         */
        application.setBannerMode(Banner.Mode.OFF);
        application.run(args);
    }
}
