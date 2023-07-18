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

package testsuite.internal.environment;

import java.sql.Connection;
import java.sql.SQLException;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.config.DriverJdbcCfg;
import static testsuite.internal.config.TestSuiteCfgPath.DEV;
import testsuite.internal.config.TestSuiteCfgReader;

public class DriverEnv extends TestSuiteEnv {

    public DriverEnv(TestSuiteShardSpec shardSpec) {
        super(shardSpec);
    }

    public DriverEnv(TestSuiteShardSpec shardSpec, String charEncoding) {
        this(shardSpec);
        super.encoding = charEncoding;
    }

    @Override
    public Connection getDevConnection() throws SQLException {
        DriverJdbcCfg cfg = TestSuiteCfgReader.read(DriverJdbcCfg.class, this.shardSpec, DEV);
        if (super.encoding != null) {
            cfg.setCharacterEncoding(super.encoding);
        }
        return getConnection(cfg.getJdbcUrl());
    }

    @Override
    public String getDevConnectionUrl() {
        DriverJdbcCfg cfg = TestSuiteCfgReader.read(DriverJdbcCfg.class, this.shardSpec, DEV);
        if (super.encoding != null) {
            cfg.setCharacterEncoding(super.encoding);
        }
        return cfg.getJdbcUrl();
    }

    @Override
    public String getKeyspace() {
        DriverJdbcCfg cfg = TestSuiteCfgReader.read(DriverJdbcCfg.class, this.shardSpec, DEV);
        return cfg.getKeyspace();
    }

    @Override
    public String getUser() {
        DriverJdbcCfg cfg = TestSuiteCfgReader.read(DriverJdbcCfg.class, this.shardSpec, DEV);
        return cfg.getUsername();
    }

    @Override
    public String getPassword() {
        DriverJdbcCfg cfg = TestSuiteCfgReader.read(DriverJdbcCfg.class, this.shardSpec, DEV);
        return cfg.getPassword();
    }
}
