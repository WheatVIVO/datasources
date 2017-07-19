package org.wheatinitiative.vivo.datasource.connector.openaire;

import javax.servlet.http.HttpServletRequest;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.service.DataSourceService;

public class OpenAireService extends DataSourceService {

	private static final OpenAire openaire = new OpenAire(); 
	
	@Override
	protected DataSource getDataSource(HttpServletRequest request) {
		return openaire;
	}
	
}