/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql;

import java.util.List;

/**
 * BindVarNeeds represents the bind vars that need to be provided as the result of expression rewriting.
 */
public class BindVarNeeds {
    private List<String> needFunctionResult;

    private List<String> needSystemVariable;

    // NeedUserDefinedVariables keeps track of all user defined variables a query is using
    private List<String> needUserDefineVariables;

    /**
     * MergeWith adds bind vars needs coming from sub scopes
     *
     * @param other
     */
    public void mergeWith(BindVarNeeds other) {
        this.needFunctionResult.addAll(other.needFunctionResult);
        this.needSystemVariable.addAll(other.needSystemVariable);
        this.needUserDefineVariables.addAll(other.needUserDefineVariables);
    }

    /**
     * AddFuncResult adds a function bindvar need
     *
     * @param name
     */
    public void addFuncResult(String name) {
        this.needFunctionResult.add(name);
    }

    /**
     * AddSysVar adds a system variable bindvar need
     *
     * @param name
     */
    public void addSysVar(String name) {
        this.needSystemVariable.add(name);
    }

    /**
     * AddUserDefVar adds a user defined variable bindvar need
     *
     * @param name
     */
    public void addUserDefVar(String name) {
        this.needUserDefineVariables.add(name);
    }

    /**
     * NeedsFuncResult says if a function result needs to be provided
     *
     * @param name
     * @return
     */
    public boolean neesFuncResult(String name) {
        for (String func : this.needFunctionResult) {
            if (func.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * NeedsSysVar says if a function result needs to be provided
     *
     * @param name
     * @return
     */
    public boolean neesSysVar(String name) {
        for (String sysVar : this.needSystemVariable) {
            if (sysVar.equals(name)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasRewrites() {
        return this.needSystemVariable.size() > 0
            || this.needFunctionResult.size() > 0
            || this.needUserDefineVariables.size() > 0;
    }
}
