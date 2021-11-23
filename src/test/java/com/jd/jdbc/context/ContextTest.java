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

package com.jd.jdbc.context;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class ContextTest {

    private static void sleep(long millsec) {
        try {
            Thread.sleep(millsec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test0() {
        Map<Object, Object> contextValues1 = new HashMap<>();
        contextValues1.put("k1", "v1");

        Map<Object, Object> contextValues2 = new HashMap<>();
        contextValues2.put("k1", "v1+");
        contextValues2.put("k2", "v2");

        IContext context1 = VtContext.withCancel(VtContext.background(), contextValues1);
        IContext context2 = VtContext.withCancel(context1, contextValues2);
        IContext context3 = VtContext.withCancel(context1);

        Assert.assertEquals("v1", context1.getContextValue("k1"));
        Assert.assertNull(context1.getContextValue("k2"));

        Assert.assertEquals("v1+", context2.getContextValue("k1"));
        Assert.assertEquals("v2", context2.getContextValue("k2"));

        Assert.assertEquals("v1", context3.getContextValue("k1"));
        Assert.assertNull(context3.getContextValue("k2"));
    }

    @Test
    public void test1() {
        IContext context1 = VtContext.withCancel(VtContext.background());

        IContext context21 = VtContext.withDeadline(context1, 2, TimeUnit.SECONDS);
        IContext context22 = VtContext.withDeadline(context1, 0, TimeUnit.SECONDS);
        IContext context23 = VtContext.withCancel(context1);

        IContext context31 = VtContext.withCancel(context21);
        IContext context32 = VtContext.withCancel(context22);
        IContext context33 = VtContext.withCancel(context23);

        Assert.assertFalse(context1.isDone());
        Assert.assertNull(context1.error());

        Assert.assertFalse(context21.isDone());
        Assert.assertNull(context21.error());

        Assert.assertTrue(context22.isDone());
        Assert.assertEquals("dead line", context22.error());

        Assert.assertFalse(context23.isDone());
        Assert.assertNull(context23.error());

        Assert.assertFalse(context31.isDone());
        Assert.assertNull(context31.error());

        Assert.assertTrue(context32.isDone());
        Assert.assertEquals("dead line", context32.error());

        Assert.assertFalse(context33.isDone());
        Assert.assertNull(context33.error());

        sleep(2500);

        Assert.assertFalse(context1.isDone());
        Assert.assertNull(context1.error());

        Assert.assertTrue(context21.isDone());
        Assert.assertEquals("dead line", context21.error());

        Assert.assertTrue(context22.isDone());
        Assert.assertEquals("dead line", context22.error());

        Assert.assertFalse(context23.isDone());
        Assert.assertNull(context23.error());

        Assert.assertTrue(context31.isDone());
        Assert.assertEquals("dead line", context31.error());

        Assert.assertTrue(context32.isDone());
        Assert.assertEquals("dead line", context32.error());

        Assert.assertFalse(context33.isDone());
        Assert.assertNull(context33.error());

        context1.close();
        Assert.assertTrue(context1.isDone());
        Assert.assertEquals("closed", context1.error());

        Assert.assertTrue(context23.isDone());
        Assert.assertEquals("closed", context23.error());

        Assert.assertTrue(context33.isDone());
        Assert.assertEquals("closed", context33.error());
    }
}
