/*
Copyright 2021 JD Project Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.vtdriver.spring.boot.autoconfigure;

import com.jd.jdbc.spring.VitessDataSourceInitializingBean;
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
    public VitessDataSourceInitializingBean vitessDataSourceInitializingBean() {
        return new VitessDataSourceInitializingBean();
    }

    @Bean
    public VtDriverSplitTable vtDriverSplitTableInit() {
        return new VtDriverSplitTable(vtDriverSplitTableProperties);
    }

}
