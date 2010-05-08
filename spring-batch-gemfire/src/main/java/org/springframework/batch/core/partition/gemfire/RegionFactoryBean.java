package org.springframework.batch.core.partition.gemfire;

import java.util.Properties;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;

public class RegionFactoryBean<K, V> implements FactoryBean<Region<K, V>>, DisposableBean {

	private DistributedSystem distributedSystem;
	private Cache cache;
	private String name = "region";

	public void destroy() throws Exception {
		if (distributedSystem != null) {
			cache.close();
			distributedSystem.disconnect();
		}
	}

	public void setName(String name) {
		this.name = name;
	}

	public Region<K, V> getObject() throws Exception {

		try {

			cache = CacheFactory.getAnyInstance();
			return cache.getRegion("/root/" + name);

		} catch (CacheClosedException e) {

			distributedSystem = DistributedSystem.connect(new Properties());
			cache = CacheFactory.create(distributedSystem);
			Region<K, V> root = cache.createRegion("root", new AttributesFactory<K, V>().create());
			AttributesFactory<K, V> attributesFactory = new AttributesFactory<K, V>(root.getAttributes());
			attributesFactory.setDataPolicy(DataPolicy.PARTITION);

			return root.createSubregion(name, attributesFactory.create());

		}

	}

	public Class<?> getObjectType() {
		return Region.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
