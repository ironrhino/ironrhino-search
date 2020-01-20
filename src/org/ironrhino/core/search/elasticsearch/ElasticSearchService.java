package org.ironrhino.core.search.elasticsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.ironrhino.core.model.ResultPage;
import org.ironrhino.core.search.SearchCriteria;
import org.ironrhino.core.search.SearchService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@SuppressWarnings(value = { "unchecked", "rawtypes" })
@Component("searchService")
public class ElasticSearchService<T> implements SearchService<T> {

	@Autowired
	private Logger logger;

	@Autowired
	private Client client;

	@Autowired
	private IndexManager indexManager;

	@Override
	public ResultPage<T> search(ResultPage<T> resultPage) {
		return search(resultPage, null);
	}

	@Override
	public ResultPage<T> search(ResultPage<T> resultPage, Mapper<T> mapper) {
		SearchCriteria criteria = resultPage.getCriteria();
		if (criteria == null)
			return resultPage;
		SearchRequestBuilder srb = criteria2builder(criteria);
		if (resultPage.isPaged()) {
			srb.setFrom(resultPage.getStart());
			srb.setSize(resultPage.getPageSize());
		} else {
			srb.setFrom(0);
			srb.setSize(ResultPage.DEFAULT_MAX_PAGESIZE);
		}
		try {
			SearchResponse response = srb.execute().get();
			SearchHits shs = response.getHits();
			if (shs != null) {
				resultPage.setTookInMillis(response.getTookInMillis());
				resultPage.setTotalResults(shs.getTotalHits());
				List list = new ArrayList(shs.getHits().length);
				resultPage.setResult(list);
				for (SearchHit sh : shs.getHits()) {
					T data = (T) indexManager.searchHitToEntity(sh);
					data = mapper == null ? data : mapper.map(data);
					if (data != null)
						list.add(data);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return resultPage;
	}

	@Override
	public List<T> search(SearchCriteria searchCriteria) {
		return search(searchCriteria, null);
	}

	@Override
	public List<T> search(SearchCriteria searchCriteria, Mapper<T> mapper) {
		return search(searchCriteria, mapper, -1);
	}

	@Override
	public List<T> search(SearchCriteria searchCriteria, Mapper mapper, int limit) {
		SearchRequestBuilder srb = criteria2builder(searchCriteria);
		srb.setFrom(0);
		if (limit > 0 && limit < ResultPage.DEFAULT_MAX_PAGESIZE)
			srb.setSize(limit);
		else
			srb.setSize(ResultPage.DEFAULT_MAX_PAGESIZE);
		List list = null;
		try {
			SearchResponse response = srb.execute().get();
			SearchHits shs = response.getHits();
			list = new ArrayList(shs.getHits().length);
			for (SearchHit sh : shs.getHits()) {
				Object data = indexManager.searchHitToEntity(sh);
				data = mapper == null ? data : mapper.map(data);
				if (data != null)
					list.add(data);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return list;
	}

	@Override
	public Map<String, Integer> countTermsByField(SearchCriteria searchCriteria, String field) {
		SearchRequestBuilder srb = criteria2builder(searchCriteria);
		srb.setFrom(0);
		srb.setSize(0);
		TermsAggregationBuilder tb = AggregationBuilders.terms(field);
		tb.field(field);
		srb.addAggregation(tb);
		try {
			SearchResponse response = srb.execute().get();
			StringTerms aggr = response.getAggregations().get(field);
			Map<String, Integer> result = new LinkedHashMap<>();
			for (Terms.Bucket bucket : aggr.getBuckets()) {
				result.put(bucket.getKey().toString(), (int) bucket.getDocCount());
			}
			return result;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return Collections.emptyMap();
	}

	private static Pattern wildcardQueryPattern = Pattern.compile("\\w+:.*[\\?\\*].*");

	private SearchRequestBuilder criteria2builder(SearchCriteria criteria) {
		String[] indices = new String[0];
		String[] types = criteria.getTypes();
		if (indices == null || indices.length == 0 && types != null && types.length > 0) {
			indices = new String[types.length];
			for (int i = 0; i < types.length; i++) {
				indices[i] = indexManager.determineIndexName(types[i]);
			}
		}
		SearchRequestBuilder srb = client.prepareSearch(indices);
		srb.setTimeout(new TimeValue(10, TimeUnit.SECONDS));
		if (types != null && types.length > 0)
			srb.setTypes(types);
		QueryBuilder qb = null;
		if (criteria instanceof ElasticSearchCriteria)
			qb = ((ElasticSearchCriteria) criteria).getQueryBuilder();
		String query = criteria.getQuery();
		if (qb == null && StringUtils.isBlank(query))
			throw new NullPointerException("queryBuilder is null and queryString is blank");
		if (qb == null && StringUtils.isNotBlank(query)) {
			if (wildcardQueryPattern.matcher(query).matches()) {
				String[] arr = query.split(":", 2);
				qb = QueryBuilders.wildcardQuery(arr[0], arr[1]);
			} else {
				QueryStringQueryBuilder qsqb = new QueryStringQueryBuilder(query);
				qsqb.defaultOperator(Operator.AND);
				qb = qsqb;
			}
		}
		srb.setQuery(qb);
		Map<String, Boolean> sorts = criteria.getSorts();
		for (Map.Entry<String, Boolean> entry : sorts.entrySet())
			srb.addSort(entry.getKey(), entry.getValue() ? SortOrder.DESC : SortOrder.ASC);
		return srb;
	}

}
