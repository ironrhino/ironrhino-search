package org.ironrhino.core.search.elasticsearch;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugin.analysis.mmseg.AnalysisMMsegPlugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.ironrhino.core.util.AppInfo;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ElasticSearchClientFactoryBean implements FactoryBean<Client>, InitializingBean, DisposableBean {

	@Value("${elasticsearch.connectString:}")
	private String connectString;

	@Value("${elasticsearch.node.name:}")
	private String nodeName;

	@Value("${elasticsearch.cluster.name:}")
	private String clusterName;

	@Value("${elasticsearch.index.analysis.analyzer.default.type:mmseg_maxword}")
	private String defaultAnalyzer;

	@Value("${elasticsearch.thread_pool.bulk.queue_size:1000}")
	private int bulkQueueSize;

	@Value("${elasticsearch.index.store.type:}")
	private String storeType;

	@Value("${elasticsearch.index.number_of_shards:1}")
	private int numberOfShards;

	@Value("${elasticsearch.index.number_of_replicas:0}")
	private int numberOfReplicas;

	private Node node;

	private Client client;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (StringUtils.isBlank(connectString)) {
			Map<String, String> nodeSettings = new HashMap<>();
			nodeSettings.put("transport.type", "local");
			nodeSettings.put("http.enabled", String.valueOf(false));
			nodeSettings.put("path.home", AppInfo.getAppHome().replace('\\', '/') + "/search");
			nodeSettings.put("node.name", StringUtils.isNotBlank(nodeName) ? nodeName : AppInfo.getInstanceId(true));
			nodeSettings.put("cluster.name", StringUtils.isNotBlank(clusterName) ? clusterName : AppInfo.getAppName());
			nodeSettings.put("thread_pool.bulk.queue_size", String.valueOf(bulkQueueSize));
			if (StringUtils.isNotBlank(storeType))
				nodeSettings.put("index.store.type", storeType);
			Constructor<Node> ctor = Node.class.getDeclaredConstructor(Environment.class, Collection.class);
			ctor.setAccessible(true);
			node = ctor.newInstance(
					InternalSettingsPreparer.prepareEnvironment(Settings.builder().put(nodeSettings).build(), null),
					Collections.singleton(AnalysisMMsegPlugin.class));
			node.start();
			client = node.client();
			Map<String, String> indexSettings = new HashMap<>();
			indexSettings.put("index.number_of_shards", String.valueOf(numberOfShards));
			indexSettings.put("index.number_of_replicas", String.valueOf(numberOfReplicas));
			indexSettings.put("index.analysis.analyzer.default.type", defaultAnalyzer);
			UpdateSettingsAction.INSTANCE.newRequestBuilder(client)
					.setSettings(Settings.builder().put(indexSettings).build()).execute();
		} else {
			Map<String, String> map = new HashMap<>();
			map.put("index.analysis.analyzer.default.type", defaultAnalyzer);
			Settings settings = Settings.builder().put(map).build();
			TransportClient tclient = new PreBuiltTransportClient(settings);
			for (String s : connectString.split("\\s*,\\s*")) {
				String arr[] = s.trim().split(":", 2);
				tclient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(arr[0]),
						arr.length == 2 ? Integer.valueOf(arr[1]) : 9300));
			}
			client = tclient;
		}
	}

	@Override
	public void destroy() throws Exception {
		client.close();
		if (node != null)
			node.close();
	}

	@Override
	public Client getObject() throws Exception {
		return client;
	}

	@Override
	public Class<? extends Client> getObjectType() {
		return Client.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}
}