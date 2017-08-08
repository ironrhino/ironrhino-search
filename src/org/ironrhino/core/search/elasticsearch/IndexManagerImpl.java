package org.ironrhino.core.search.elasticsearch;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.ironrhino.core.coordination.LockService;
import org.ironrhino.core.metadata.Trigger;
import org.ironrhino.core.model.Persistable;
import org.ironrhino.core.search.elasticsearch.annotations.Index;
import org.ironrhino.core.search.elasticsearch.annotations.Searchable;
import org.ironrhino.core.search.elasticsearch.annotations.SearchableComponent;
import org.ironrhino.core.search.elasticsearch.annotations.SearchableId;
import org.ironrhino.core.search.elasticsearch.annotations.SearchableProperty;
import org.ironrhino.core.search.elasticsearch.annotations.Store;
import org.ironrhino.core.service.EntityManager;
import org.ironrhino.core.util.AnnotationUtils;
import org.ironrhino.core.util.ClassScanner;
import org.ironrhino.core.util.DateUtils;
import org.ironrhino.core.util.JsonUtils;
import org.ironrhino.core.util.ReflectionUtils;
import org.slf4j.Logger;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;

@SuppressWarnings(value = { "unchecked", "rawtypes" })
@Component
public class IndexManagerImpl implements IndexManager {

	private static final String INDEX_PREFIX = "index_";

	@Autowired
	private Logger logger;

	private Map<String, Class> typeClassMapping;

	private Map<Class, Map<String, Object>> schemaMapping;

	@Autowired
	private LockService lockService;

	@Autowired
	private Client client;

	@Autowired
	private EntityManager entityManager;

	private ObjectMapper objectMapper;

	@PostConstruct
	public void init() {
		objectMapper = JsonUtils.createNewObjectMapper();
		objectMapper.setDateFormat(new SimpleDateFormat(DateUtils.DATETIME_ISO));
		objectMapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {

			private static final long serialVersionUID = -2795053276465297328L;

			@Override
			protected boolean _isIgnorable(Annotated a) {
				if (a.getAnnotation(SearchableId.class) != null || a.getAnnotation(SearchableProperty.class) != null
						|| a.getAnnotation(SearchableComponent.class) != null)
					return false;
				return super._isIgnorable(a);
			}

		});
		Collection<Class<?>> set = ClassScanner.scanAnnotated(ClassScanner.getAppPackages(), Searchable.class);
		typeClassMapping = new HashMap<>(set.size());
		schemaMapping = new HashMap<>(set.size());
		for (Class c : set) {
			Searchable searchable = (Searchable) c.getAnnotation(Searchable.class);
			if (!searchable.root() || c.getSimpleName().contains("$"))
				continue;
			typeClassMapping.put(classToType(c), c);
			schemaMapping.put(c, getSchemaMapping(c, false));
		}
		initialize();
		if (client instanceof NodeClient) {
			NodeClient nc = (NodeClient) client;
			if ("mmapfs".equals(nc.settings().get("index.store.type")))
				new Thread(this::rebuild).start();
		}
	}

	private static Map<String, Object> getSchemaMapping(Class c, boolean component) {
		Map<String, Object> mapping = new HashMap<>();
		Map<String, Object> properties = new HashMap<>();
		if (component)
			mapping.put("type", "object");
		mapping.put("properties", properties);
		PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(c);
		for (PropertyDescriptor pd : pds) {
			String name = pd.getName();
			Method m = pd.getReadMethod();
			Class propertyType = pd.getPropertyType();
			if (propertyType == null)
				continue;
			Class componentType = propertyType;
			if (propertyType.isArray()) {
				componentType = propertyType.getComponentType();
				if (componentType.isInterface())
					continue;
			} else if (Collection.class.isAssignableFrom(propertyType)) {
				Type type = m.getGenericReturnType();
				if (type instanceof ParameterizedType) {
					ParameterizedType ptype = (ParameterizedType) type;
					Type temp = ptype.getActualTypeArguments()[0];
					if (temp instanceof Class)
						componentType = (Class) temp;
				}
				if (componentType.isInterface())
					continue;
			}

			SearchableId searchableId = null;
			SearchableProperty searchableProperty = null;
			SearchableComponent searchableComponent = null;
			if (m != null) {
				searchableId = m.getAnnotation(SearchableId.class);
				searchableProperty = m.getAnnotation(SearchableProperty.class);
				searchableComponent = m.getAnnotation(SearchableComponent.class);
			}
			try {
				Field f = pd.getReadMethod().getDeclaringClass().getDeclaredField(name);
				if (f != null) {
					if (searchableId == null)
						searchableId = f.getAnnotation(SearchableId.class);
					if (searchableProperty == null)
						searchableProperty = f.getAnnotation(SearchableProperty.class);
					if (searchableComponent == null)
						searchableComponent = f.getAnnotation(SearchableComponent.class);
				}
			} catch (Exception e) {
			}
			if (searchableId != null) {
				properties.put(name, new PropertyMapping(componentType, searchableId));
			} else if (searchableProperty != null) {
				properties.put(name, new PropertyMapping(componentType, searchableProperty));
			} else if (searchableComponent != null) {
				properties.put(name, getSchemaMapping(componentType, true));
			}
		}
		return mapping;
	}

	public static class PropertyMapping {
		private String type = "text";
		private String index_name;
		private String format;
		private Float boost;
		private Boolean index;
		private Boolean store;
		private String analyzer;
		private String search_analyzer;
		private Boolean include_in_all;
		private String null_value;
		private String term_vector;
		private Boolean omit_norms;
		private Boolean omit_term_freq_and_positions;
		private Boolean ignore_malformed;

		public PropertyMapping() {

		}

		public PropertyMapping(Class propertyClass, SearchableId searchableProperty) {
			this.type = "keyword";
			if (StringUtils.isNotBlank(searchableProperty.index_name()))
				this.index_name = searchableProperty.index_name();
			if (StringUtils.isNotBlank(searchableProperty.format()))
				this.format = searchableProperty.format();
			Index index = searchableProperty.index();
			if (index == Index.NO)
				this.index = false;
			if (("text".equals(this.type) || "keyword".equals(this.type))
					&& searchableProperty.index() != Index.NOT_ANALYZED && searchableProperty.index() != Index.NO
					&& !searchableProperty.omit_norms() && searchableProperty.boost() != 1.0f)
				this.boost = searchableProperty.boost();
			Store store = searchableProperty.store();
			if (store != Store.NA)
				this.store = store == Store.YES;
			if (StringUtils.isNotBlank(searchableProperty.analyzer()))
				this.analyzer = searchableProperty.analyzer();
			if (StringUtils.isNotBlank(searchableProperty.search_analyzer()))
				this.search_analyzer = searchableProperty.search_analyzer();
			if (!searchableProperty.include_in_all())
				this.include_in_all = false;
			if (StringUtils.isNotBlank(searchableProperty.null_value()))
				this.null_value = searchableProperty.null_value();
			if (searchableProperty.omit_norms())
				this.omit_norms = searchableProperty.omit_norms();
			if (searchableProperty.omit_term_freq_and_positions())
				this.omit_term_freq_and_positions = searchableProperty.omit_term_freq_and_positions();
			if ("date".equals(this.type) || StringUtils.isNotBlank(this.format))
				this.ignore_malformed = searchableProperty.ignore_malformed();

		}

		public PropertyMapping(Class propertyClass, SearchableProperty searchableProperty) {
			this.type = searchableProperty.type();
			if (StringUtils.isBlank(type)) {
				if (propertyClass.isPrimitive())
					this.type = propertyClass.toString();
				else if (propertyClass.isEnum())
					this.type = "keyword";
				else
					this.type = propertyClass.getSimpleName().toLowerCase(Locale.ROOT);
			}
			this.type = translateType(this.type);
			if (StringUtils.isNotBlank(searchableProperty.index_name()))
				this.index_name = searchableProperty.index_name();
			if (StringUtils.isNotBlank(searchableProperty.format()))
				this.format = searchableProperty.format();
			Index index = searchableProperty.index();
			if (index == Index.NO)
				this.index = false;
			else if (index == Index.ANALYZED)
				this.type = "text";
			else if (index == Index.NOT_ANALYZED)
				this.type = "keyword";
			if (("text".equals(this.type) || "keyword".equals(this.type))
					&& searchableProperty.index() != Index.NOT_ANALYZED && searchableProperty.index() != Index.NO
					&& !searchableProperty.omit_norms() && searchableProperty.boost() != 1.0f)
				this.boost = searchableProperty.boost();
			Store store = searchableProperty.store();
			if (store != Store.NA)
				this.store = store == Store.YES;
			if ("text".equals(this.type)) {
				if (StringUtils.isNotBlank(searchableProperty.analyzer()))
					this.analyzer = searchableProperty.analyzer();
				if (StringUtils.isNotBlank(searchableProperty.search_analyzer()))
					this.search_analyzer = searchableProperty.search_analyzer();
				if (StringUtils.isBlank(this.analyzer))
					this.analyzer = "mmseg_maxword";
			}
			if (!searchableProperty.include_in_all())
				this.include_in_all = false;
			if (StringUtils.isNotBlank(searchableProperty.null_value()))
				this.null_value = searchableProperty.null_value();
			if (searchableProperty.omit_norms())
				this.omit_norms = searchableProperty.omit_norms();
			if (searchableProperty.omit_term_freq_and_positions())
				this.omit_term_freq_and_positions = searchableProperty.omit_term_freq_and_positions();
			if ("date".equals(this.type) || StringUtils.isNotBlank(this.format))
				this.ignore_malformed = searchableProperty.ignore_malformed();
		}

		private static String translateType(String input) {
			if (input.equals("int"))
				return "integer";
			if (input.equals("bigdecimal"))
				return "double";
			if (input.equals("string"))
				return "text";
			return input;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getFormat() {
			return format;
		}

		public void setFormat(String format) {
			this.format = format;
		}

		public Float getBoost() {
			return boost;
		}

		public void setBoost(Float boost) {
			this.boost = boost;
		}

		public Boolean getIndex() {
			return index;
		}

		public void setIndex(Boolean index) {
			this.index = index;
		}

		public Boolean getStore() {
			return store;
		}

		public void setStore(Boolean store) {
			this.store = store;
		}

		public String getAnalyzer() {
			return analyzer;
		}

		public void setAnalyzer(String analyzer) {
			this.analyzer = analyzer;
		}

		public Boolean getInclude_in_all() {
			return include_in_all;
		}

		public void setInclude_in_all(Boolean include_in_all) {
			this.include_in_all = include_in_all;
		}

		public String getIndex_name() {
			return index_name;
		}

		public void setIndex_name(String index_name) {
			this.index_name = index_name;
		}

		public String getSearch_analyzer() {
			return search_analyzer;
		}

		public void setSearch_analyzer(String search_analyzer) {
			this.search_analyzer = search_analyzer;
		}

		public String getNull_value() {
			return null_value;
		}

		public void setNull_value(String null_value) {
			this.null_value = null_value;
		}

		public String getTerm_vector() {
			return term_vector;
		}

		public void setTerm_vector(String term_vector) {
			this.term_vector = term_vector;
		}

		public Boolean getOmit_norms() {
			return omit_norms;
		}

		public void setOmit_norms(Boolean omit_norms) {
			this.omit_norms = omit_norms;
		}

		public Boolean getOmit_term_freq_and_positions() {
			return omit_term_freq_and_positions;
		}

		public void setOmit_term_freq_and_positions(Boolean omit_term_freq_and_positions) {
			this.omit_term_freq_and_positions = omit_term_freq_and_positions;
		}

		public Boolean getIgnore_malformed() {
			return ignore_malformed;
		}

		public void setIgnore_malformed(Boolean ignore_malformed) {
			this.ignore_malformed = ignore_malformed;
		}

	}

	private String entityToDocument(Persistable entity) {
		Map<String, Object> map = AnnotationUtils.getAnnotatedPropertyNameAndValues(entity, SearchableId.class,
				SearchableProperty.class, SearchableComponent.class);
		Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Object value = it.next().getValue();
			if (value == null || value instanceof String && StringUtils.isBlank((String) value)
					|| value instanceof Collection && ((Collection) value).isEmpty()
					|| value.getClass().isArray() && ((Object[]) value).length == 0)
				it.remove();
		}
		if (map.isEmpty())
			logger.warn("{} is empty", entity);
		try {
			return objectMapper.writeValueAsString(map);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	private static String classToType(Class clazz) {
		String type = StringUtils.uncapitalize(clazz.getSimpleName());
		Searchable s = (Searchable) clazz.getAnnotation(Searchable.class);
		if (s != null && StringUtils.isNotBlank(s.type()))
			type = s.type();
		return type;
	}

	private Class typeToClass(String type) {
		return typeClassMapping.get(type);
	}

	@Override
	public String determineIndexName(String type) {
		return INDEX_PREFIX + type.toLowerCase();
	}

	@Override
	public Object searchHitToEntity(SearchHit sh) throws Exception {
		return objectMapper.readValue(sh.getSourceAsString(), typeToClass(sh.getType()));
	}

	@Override
	public ListenableActionFuture<IndexResponse> index(Persistable entity) {
		String type = classToType(ReflectionUtils.getActualClass(entity));
		return client.prepareIndex(determineIndexName(type), type, String.valueOf(entity.getId()))
				.setSource(entityToDocument(entity), XContentType.JSON).execute();
	}

	@Override
	public ListenableActionFuture<DeleteResponse> delete(Persistable entity) {
		String type = classToType(ReflectionUtils.getActualClass(entity));
		return client.prepareDelete(determineIndexName(type), type, String.valueOf(entity.getId())).execute();
	}

	private void initialize() {
		IndicesAdminClient adminClient = client.admin().indices();
		for (Map.Entry<Class, Map<String, Object>> entry : schemaMapping.entrySet()) {
			HashMap<String, Map<String, Object>> map = new HashMap<>();
			String type = classToType(entry.getKey());
			try {
				IndicesExistsResponse ies = adminClient.exists(new IndicesExistsRequest(determineIndexName(type)))
						.get();
				if (!ies.isExists())
					adminClient.create(new CreateIndexRequest(determineIndexName(type))).get();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			map.put(type, entry.getValue());
			String mapping = JsonUtils.toJson(map);
			if (logger.isDebugEnabled())
				logger.debug("Mapping {} : {}", entry.getKey(), mapping);
			try {
				adminClient.preparePutMapping(determineIndexName(type)).setType(type)
						.setSource(mapping, XContentType.JSON).execute().get();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	@Override
	@Trigger
	public void rebuild() {
		String lockName = "indexManager.rebuild()";
		if (lockService.tryLock(lockName)) {
			try {
				IndicesAdminClient adminClient = client.admin().indices();
				for (Map.Entry<Class, Map<String, Object>> entry : schemaMapping.entrySet()) {
					String type = classToType(entry.getKey());
					try {
						IndicesExistsResponse ies = adminClient
								.exists(new IndicesExistsRequest(determineIndexName(type))).get();
						if (!ies.isExists())
							adminClient.delete(new DeleteIndexRequest(determineIndexName(type))).get();
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				}
				initialize();
				for (Class c : schemaMapping.keySet())
					indexAll(classToType(c));
				logger.info("rebuild completed");
			} finally {
				lockService.unlock(lockName);
			}
		}
	}

	@Override
	public void indexAll(String type) {
		Class clz = typeToClass(type);
		entityManager.setEntityClass(clz);
		final AtomicLong indexed = new AtomicLong();
		entityManager.iterate(20, (entityArray, session) -> {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			indexed.addAndGet(entityArray.length);
			for (Object obj : entityArray) {
				Persistable p = (Persistable) obj;
				bulkRequest.add(client.prepareIndex(determineIndexName(type), type, String.valueOf(p.getId()))
						.setSource(entityToDocument(p), XContentType.JSON));
			}
			try {
				if (bulkRequest.numberOfActions() > 0) {
					ListenableActionFuture<BulkResponse> br = bulkRequest.execute();
					br.addListener(bulkResponseActionListener);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		logger.info("indexed {} for {}", indexed.get(), type);
	}

	private ActionListener<BulkResponse> bulkResponseActionListener = new ActionListener<BulkResponse>() {
		@Override
		public void onResponse(BulkResponse br) {
			if (br.hasFailures())
				logger.error(br.buildFailureMessage());
		}

		@Override
		public void onFailure(Exception e) {
			logger.error(e.getMessage(), e);
		}
	};

}
