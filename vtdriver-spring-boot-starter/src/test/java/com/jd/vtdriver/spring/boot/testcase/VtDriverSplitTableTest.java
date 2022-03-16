package com.jd.vtdriver.spring.boot.testcase;

import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.SplitTableUtil;
import com.jd.vtdriver.spring.boot.demo.VtdriverSpringbootApplication;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = VtdriverSpringbootApplication.class)
@RunWith(SpringRunner.class)
public class VtDriverSplitTableTest {

    @Test
    public void testSplitTableConfig() {
        try {
            Field field = SplitTableUtil.class.getDeclaredField("tableIndexesMap");
            field.setAccessible(true);
            Map<String, Map<String, LogicTable>> actualMap = (Map<String, Map<String, LogicTable>>) field.get(SplitTableUtil.class);
            Assert.assertEquals(2, actualMap.size());

            Assert.assertNotNull(actualMap.get("customer"));
            Map<String, LogicTable> customer = actualMap.get("customer");
            Assert.assertEquals(1, customer.size());

            Assert.assertNotNull(customer.get("table_engine_test"));
            Assert.assertEquals("table_engine_test", customer.get("table_engine_test").getLogicTable());
            Assert.assertEquals(4, customer.get("table_engine_test").getActualTableList().size());
            Assert.assertEquals("f_key", customer.get("table_engine_test").getTindexCol().getColumnName());
            Assert.assertEquals(263, customer.get("table_engine_test").getTindexCol().getType().getNumber());
            Assert.assertEquals("com.jd.jdbc.tindexes.TableRuleMod", customer.get("table_engine_test").getTableIndex().getClass().getName());

            Assert.assertNotNull(actualMap.get("commerce"));
            Map<String, LogicTable> commerce = actualMap.get("commerce");
            Assert.assertEquals(1, commerce.size());

            Assert.assertNotNull(commerce.get("table_engine_test"));
            Assert.assertEquals("table_engine_test", commerce.get("table_engine_test").getLogicTable());
            Assert.assertEquals(4, commerce.get("table_engine_test").getActualTableList().size());
            Assert.assertEquals("f_key", commerce.get("table_engine_test").getTindexCol().getColumnName());
            Assert.assertEquals(263, commerce.get("table_engine_test").getTindexCol().getType().getNumber());
            Assert.assertEquals("com.jd.jdbc.tindexes.TableRuleMod", commerce.get("table_engine_test").getTableIndex().getClass().getName());

        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
