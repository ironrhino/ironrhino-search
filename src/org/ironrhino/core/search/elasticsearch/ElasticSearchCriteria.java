package org.ironrhino.core.search.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;
import org.ironrhino.core.search.SearchCriteria;

public class ElasticSearchCriteria extends SearchCriteria {

	private static final long serialVersionUID = 2810417180615970724L;

	private QueryBuilder queryBuilder;

	public QueryBuilder getQueryBuilder() {
		return queryBuilder;
	}

	public void setQueryBuilder(QueryBuilder queryBuilder) {
		this.queryBuilder = queryBuilder;
	}

}
