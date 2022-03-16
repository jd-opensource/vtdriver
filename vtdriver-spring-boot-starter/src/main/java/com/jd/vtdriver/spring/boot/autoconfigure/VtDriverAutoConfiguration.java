package com.jd.vtdriver.spring.boot.autoconfigure;

import com.jd.vtdriver.spring.boot.autoconfigure.properties.VtDriverSplitTableProperties;
import javax.annotation.Resource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(VtDriverSplitTableProperties.class)
public class VtDriverAutoConfiguration {

    @Resource
    private VtDriverSplitTableProperties vtDriverSplitTableProperties;

    @Bean
    public VtDriverDataSourceInit vtDriverDataSourceInit() {
        return new VtDriverDataSourceInit();
    }

    @Bean
    public VtDriverSplitTable vtDriverSplitTableInit() {
        return new VtDriverSplitTable(vtDriverSplitTableProperties);
    }

}
