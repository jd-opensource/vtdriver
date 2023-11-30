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

package com.jd.jdbc.planbuilder.gen4.operator.physical;

import com.jd.jdbc.engine.Engine;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
// Cost is used to make it easy to compare the Cost of two plans with each other
public class Cost {

    private int vindexCost;

    private boolean isUnique;

    private Engine.RouteOpcode opCode;
}
