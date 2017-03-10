package org.wheatinitiative.vivo.datasource.connector.cordis;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.CsvDataSource;
import org.wheatinitiative.vivo.datasource.util.csv.CsvToRdf;

import com.hp.hpl.jena.rdf.model.Model;

public class Cordis extends CsvDataSource implements DataSource {

	public Cordis() {
		// TODO Auto-generated constructor stub
	}

	@Override
	protected CsvToRdf getCsvConverter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getABoxNamespaceAndPrefix() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Model filter(Model model) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Model mapToVIVO(Model model) {
		// TODO Auto-generated method stub
		return null;
	}

}
