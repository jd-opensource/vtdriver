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

package com.jd.jdbc.util;

import com.jd.jdbc.util.threadpool.JdkUtil;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class JdkUtilTest {

    private List<JdkUtilNode> jdkUtilNodeList;

    @Test
    public void test00() throws NoSuchFieldException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        initJdkData();
        int i = 0;
        for (JdkUtilNode jdkNode : jdkUtilNodeList) {
            i++;
            System.out.println("No. " + i);
            System.out.println(" availableProcessors: " + jdkNode.availableProcessors
                + " expectPoolCoreSize: " + jdkNode.expectPoolCoreSize
                + " poolCoreSize: " + jdkNode.poolCoreSize);
            Assert.assertEquals("Expected: " + jdkNode.expectPoolCoreSize + "; Actual: " + jdkNode.poolCoreSize, jdkNode.expectPoolCoreSize, jdkNode.poolCoreSize);
        }
        recoverJdkUtilVariable();
    }

    private void recoverJdkUtilVariable() throws NoSuchFieldException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        new JdkUtilNode(Runtime.getRuntime().availableProcessors(), -1);
    }

    private void initJdkData() throws NoSuchFieldException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        jdkUtilNodeList = new ArrayList<JdkUtilNode>();
        jdkUtilNodeList.add(new JdkUtilNode(10, 10));
        jdkUtilNodeList.add(new JdkUtilNode(6, 8));
        jdkUtilNodeList.add(new JdkUtilNode(36, 32));
        jdkUtilNodeList.add(new JdkUtilNode(8, 8));
        jdkUtilNodeList.add(new JdkUtilNode(32, 32));
    }

    class JdkUtilNode {
        private int availableProcessors;

        private int poolCoreSize;

        private int expectPoolCoreSize;

        JdkUtilNode(int availableProcessors, int expectPoolCoreSize)
            throws NoSuchFieldException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
            this.availableProcessors = availableProcessors;
            this.expectPoolCoreSize = expectPoolCoreSize;
            setQueryCorePoolSize();
        }

        private void setQueryCorePoolSize() throws NoSuchFieldException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
            Field availableProcessorsField = JdkUtil.class.getDeclaredField("availableProcessors");
            changeStaticFinal(availableProcessorsField, this.availableProcessors);
            Assert.assertEquals("Expected: " + this.availableProcessors + "; Actual: " + availableProcessorsField.get(null), this.availableProcessors, availableProcessorsField.get(null));
            Method initQueryExecutorCorePoolSizeMethod = JdkUtil.class.getDeclaredMethod("initQueryExecutorCorePoolSize");
            initQueryExecutorCorePoolSizeMethod.setAccessible(true);
            initQueryExecutorCorePoolSizeMethod.invoke(null);
            this.poolCoreSize = JdkUtil.getQueryExecutorCorePoolSize();
        }

        private void changeStaticFinal(Field field, Object value) throws NoSuchFieldException, IllegalAccessException {
            field.setAccessible(true);
            Field modiField = Field.class.getDeclaredField("modifiers");
            modiField.setAccessible(true);
            modiField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, value);
        }
    }
}
