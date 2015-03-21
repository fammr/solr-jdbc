package net.unit8.solr.jdbc.impl;

import java.sql.SQLException;
import java.util.List;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import net.unit8.solr.jdbc.expression.Expression;

public class DefaultResultSetImpl extends AbstractResultSet {
	public DefaultResultSetImpl(SolrDocumentList solrResult, List<Expression> expressions) {
		this.docList = solrResult;
		this.metaData = new ResultSetMetaDataImpl(this, expressions, null);

		if (metaData.getCountColumnList().size() > 0) {
			// Countがあれば0件でもResultSetが帰るため
			if(docList.size() == 0) {
				docList.add(new SolrDocument()); 
			}
			for(SolrDocument doc : docList) {
				for(Expression countColumn :metaData.getCountColumnList()) {
					doc.setField(countColumn.getResultName(), solrResult.getNumFound());
				}
			}
		}
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return null;
	}
}
