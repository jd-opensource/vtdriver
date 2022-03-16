package com.jd.vtdriver.spring.boot.testcase;


import com.jd.vtdriver.spring.boot.demo.VtdriverSpringbootApplication;
import com.jd.vtdriver.spring.boot.demo.mapper.TableMapper;
import com.jd.vtdriver.spring.boot.demo.mapper.UserMapper;
import com.jd.vtdriver.spring.boot.demo.model.Table;
import com.jd.vtdriver.spring.boot.demo.model.User;
import java.util.List;
import javax.annotation.Resource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = VtdriverSpringbootApplication.class)
@RunWith(SpringRunner.class)
@ActiveProfiles("yaml")
public class DemoTest {

    @Resource
    private UserMapper userMapper;

    @Resource
    private TableMapper tableMapper;

    @Test
    public void testUser() {
        List<User> all = userMapper.getAll();
        all.forEach(System.out::println);
    }

    @Test
    public void testSplitTablePopWare() {
        List<Table> all = tableMapper.getTen();
        System.out.println(all.size());
    }
}
