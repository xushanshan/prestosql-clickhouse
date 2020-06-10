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

import com.google.inject.*;
import com.google.inject.Module;
import io.prestosql.plugin.jdbc.*;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.SQLException;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * @author xushanshan
 */
public class ClickhouseClientModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ClickhouseClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ClickhouseConfig.class);
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, ClickhouseConfig clickhouseConfig) throws SQLException {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", Boolean.toString(clickhouseConfig.isDriverUseInformationSchema()));
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (clickhouseConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(clickhouseConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(
                new ClickHouseDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
}
