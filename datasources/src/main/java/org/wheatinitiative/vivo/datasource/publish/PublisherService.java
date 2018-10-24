package org.wheatinitiative.vivo.datasource.publish;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceServiceMultipleInstance;

public class PublisherService extends DataSourceServiceMultipleInstance {

    private static final long serialVersionUID = 1L;

    @Override
    protected DataSource constructDataSource() {
        return new Publisher();
    }
    
}
