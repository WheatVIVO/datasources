package org.wheatinitiative.vivo.datasource.service;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;

/**
 * A DataSourceService that allows a single servlet to use multiple DataSource
 * worker instances based on the specific request URI.  E.g., a servlet mapping
 * with a wildcard can be used to dynamically create additional instances.
 * To prevent memory exhaustion, the maximum number of DataSource instances
 * is limited by the DATA_SOURCE_MAX_INSTANCES constant.
 * @author Brian Lowe
 */
public abstract class DataSourceServiceMultipleInstance extends DataSourceService {

    // Throw an exception instead of creating a new instance if the number 
    // already created equals the following limit.
    private static final int DATA_SOURCE_MAX_INSTANCES = 256;
    
    private static Map<String, DataSource> dataSourceInstances =
            new HashMap<String, DataSource>();
    
    @Override
    protected final DataSource getDataSource(HttpServletRequest request) {
        return getDataSourceForAddress(request.getRequestURI());
    }
    
    /*
     * To be overriden by subclasses:  return a newly-constructed instance
     * of the appropriate data source implementation.
     */
    protected abstract DataSource constructDataSource();
    
    private synchronized DataSource getDataSourceForAddress(String address) {
        DataSource dataSource = dataSourceInstances.get(address);
        if(dataSource != null) {
            return dataSource;
        } else {
            // Should never be greater than, but let's be safe
            if(dataSourceInstances.size() >= DATA_SOURCE_MAX_INSTANCES) {
                throw new RuntimeException(
                        "Unable to construct data source instance for address " 
                                + address + ".  Maximum of " 
                                + DATA_SOURCE_MAX_INSTANCES 
                                + " reached for this service");
            } else {
                dataSource = constructDataSource();
                dataSourceInstances.put(address, dataSource);
                return dataSource;
            }
        }
    }

}
