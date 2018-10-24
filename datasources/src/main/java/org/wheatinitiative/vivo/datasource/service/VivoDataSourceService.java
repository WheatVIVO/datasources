package org.wheatinitiative.vivo.datasource.service;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

public class VivoDataSourceService extends DataSourceServiceMultipleInstance {

    private static final long serialVersionUID = 1L;

    @Override
    protected DataSource constructDataSource() {
        return new VivoDataSource();
    }

}
