package com.jd.jdbc.concurrency;

import com.jd.jdbc.Executor;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtCancelContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.VitessStatement;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class AllErrorRecorderTest extends TestSuite {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testNull() throws SQLException, NoSuchFieldException, IllegalAccessException {
        VitessConnection conn = (VitessConnection) getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        VitessStatement stmt = (VitessStatement) conn.createStatement();
        stmt.executeUpdate("delete from user");
        stmt.executeUpdate("insert into user (id, name) values (1, 'name1'), (2, 'name2'), (3, 'name3')");
        stmt.close();

        // 构造一个执行一次isDone就过期的context
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(VtCancelContext.class);
        enhancer.setCallback(new MethodInterceptor() {
            private int count;

            @Override
            public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
                if (method.getName().equals("isDone")) {
                    count++;
                    if (count > 1) {
                        ((IContext) obj).cancel("dead line");
                    }
                }
                return proxy.invokeSuper(obj, args);
            }
        });

        Class[] argsClass = new Class[]{IContext.class, Map.class};
        Object[] args = new Object[]{VtContext.withCancel(conn.getCtx()), new HashMap<>()};
        IContext ctx = (IContext) enhancer.create(argsClass, args);
        ctx.setContextValue(VitessPropertyKey.MAX_ROWS.getKeyName(),
            Integer.valueOf(conn.getProperties().getProperty(VitessPropertyKey.MAX_ROWS.getKeyName(), "0")));

        // 构造一个Statement, 拦截 executeQueryInternal 方法
        enhancer = new Enhancer();
        enhancer.setSuperclass(VitessStatement.class);
        enhancer.setCallback(new MethodInterceptor() {
            @Override
            public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
                if (method.getName().equals("executeQueryInternal")) {
                    // 用构造的 ctx 替换executeQueryInternal方法的第一个参数
                    args[0] = ctx;
                }
                return proxy.invokeSuper(obj, args);
            }
        });
        argsClass = new Class[]{VitessConnection.class, Executor.class};
        args = new Object[]{conn, Executor.getInstance(null)};
        VitessStatement stmt2 = (VitessStatement) enhancer.create(argsClass, args);

        thrown.expect(java.sql.SQLException.class);
        thrown.expectMessage("execution is cancelled. dead line");

        ResultSet rs = stmt2.executeQuery("select id, name from user");
        // 在 NativQueryService.execute 里被拦下来, 不会走到这一行
        printNormal("columnCount: " + rs.getMetaData().getColumnCount());

        rs.close();
        stmt2.close();
        conn.close();
    }
}
