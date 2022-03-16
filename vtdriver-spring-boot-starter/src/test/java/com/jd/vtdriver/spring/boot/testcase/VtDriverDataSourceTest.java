package com.jd.vtdriver.spring.boot.testcase;

import com.jd.vtdriver.spring.boot.autoconfigure.VtDriverDataSourceInit;
import com.jd.vtdriver.spring.boot.demo.VtdriverSpringbootApplication;
import javax.annotation.Resource;
import javax.sql.DataSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = VtdriverSpringbootApplication.class)
@RunWith(SpringRunner.class)
public class VtDriverDataSourceTest {
    @Resource
    private DataSource dataSource;

    @Test
    public void testIsVtDriverDataSource() {
        Assert.assertTrue(VtDriverDataSourceInit.isVtDriverDataSource(dataSource));
    }
}
