/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.clickhouse;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * @author xushanshan
 */
public class ClickhouseConfig {
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    private boolean driverUseInformationSchema = true;

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @Config("clickhouse.connection-timeout")
    public ClickhouseConfig setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema() {
        return driverUseInformationSchema;
    }

    @Config("clickhouse.jdbc.use-information-schema")
    @ConfigDescription("Value of useInformationSchema ClickHouse JDBC driver connection property")
    public ClickhouseConfig setDriverUseInformationSchema(boolean driverUseInformationSchema) {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }
}
