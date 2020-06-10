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
package com.test;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.clickhouse.ClickhousePlugin;
import io.prestosql.plugin.jdbc.JdbcConnector;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.junit.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestClickhousePlugin {

    @Test
    public void createClickhousePlugin() {
        String url = "jdbc:clickhouse://localhost:8123";
        Plugin plugin = new ClickhousePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Connector connector = factory.create("test-clickhouse", ImmutableMap.of("connection-url", url), new TestingConnectorContext());

        if (connector instanceof JdbcConnector) {
            JdbcConnector clickhouseConnector = (JdbcConnector)connector;
            clickhouseConnector.getSystemTables();

        }

        connector.shutdown();
    }
}
