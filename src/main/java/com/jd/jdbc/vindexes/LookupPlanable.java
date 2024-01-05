/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

package com.jd.jdbc.vindexes;

import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;

// LookupPlanable are for lookup vindexes where we can extract the lookup query at plan time
public interface LookupPlanable {
    String String();

    String[] Query(); // (selQuery string, arguments []string)

    Destination[] MapResult(VtValue[] ids, VtResultSet[] results);

    Boolean AllowBatch();

    Object GetCommitOrder();
}
