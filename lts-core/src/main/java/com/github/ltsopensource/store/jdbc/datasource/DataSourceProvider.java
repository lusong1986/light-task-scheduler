package com.github.ltsopensource.store.jdbc.datasource;

import javax.sql.DataSource;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.constant.ExtConfig;
import com.github.ltsopensource.core.spi.SPI;

/**
 * @author Robert HG (254963746@qq.com) on 10/24/14.
 */
@SPI(key = ExtConfig.JDBC_DATASOURCE_PROVIDER, dftValue = "mysql")
public interface DataSourceProvider {

	DataSource getDataSource(Config config);

}
