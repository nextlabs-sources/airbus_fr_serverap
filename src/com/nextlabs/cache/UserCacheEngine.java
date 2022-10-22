package com.nextlabs.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ehcache.Cache;
import org.ehcache.Cache.Entry;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;

import com.nextlabs.common.UserObject;

/**
 * This class implement the CacheEngine for storing and retrieving user object.
 * @author klee
 *
 */
public class UserCacheEngine {

	public static final String USER_CACHE_NAME = "UserAttributeProviderCache";
	private static final Log LOG = LogFactory.getLog(UserCacheEngine.class);
	private static UserCacheEngine engine;
	private CacheManager userCacheManager;
	private Cache<String, UserObject> userObjectCache;

	private Map<String, String> identifierMap;

	public UserCacheEngine() {
	}

	/**
	 * Singleton method to get CacheEngine
	 * @return Object of UserCacheEngine
	 */
	public static UserCacheEngine getInstance() {
		if (engine == null) {
			engine = new UserCacheEngine();
		}
		return engine;
	}

	/**
	 * Writing UserObject into user cache
	 * @param obj UserObject
	 */
	public void writeObjectToUserCache(UserObject obj) {
		if (userObjectCache == null) {
			LOG.error("Cache has not been initialized");
			return;
		}
		userObjectCache.put(obj.getId(), obj);
	}
	
	/**
	 * Initialize the cache with the properties parameter such as cache_heap_in_mb, cache_max_object and etc
	 * @param props Properties contain parameter for cache setting
	 */
	public void initializeCache(Properties props) {
		ResourcePoolsBuilder resourceBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
		
		String cacheHeap = props.getProperty("cache_heap_in_mb", "128");
		LOG.info(String.format("cache_heap_in_mb will be set to [%s] MB", cacheHeap)); 
		int iHeapMem = 128;
		
		try{
			iHeapMem = Integer.parseInt(cacheHeap);
		}catch(Exception ex){
			LOG.error("Not able to read cache_heap_in_mb, hard set to 128MB");
		}
		resourceBuilder = resourceBuilder.heap(iHeapMem, MemoryUnit.MB);
		LOG.info(String.format("cache_heap_in_mb will be set to [%s] MB", iHeapMem));
		
		
		String cache_max_object = props.getProperty("cache_max_object","5000");
		long lCache_max_object = 5000;
		try{
			lCache_max_object = Long.parseLong(cache_max_object);
		}catch(Exception ex){
			LOG.error("Not able to read cache_max_object, hard set to 5000");
		}
		
		LOG.info(String.format("cache_max_object will be set to [%s]", lCache_max_object));	

		String timeToLive = props.getProperty("time_to_live", "1_DAYS");
		
		Duration duration = getTimeToLive(timeToLive);
		
		//User Cache region
		CacheConfigurationBuilder<String, UserObject> userCacheConfigurationBuilder = CacheConfigurationBuilder
				.newCacheConfigurationBuilder(String.class, UserObject.class, resourceBuilder)
				.withExpiry(Expirations.timeToLiveExpiration(duration)).withSizeOfMaxObjectGraph(lCache_max_object);

		CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
		cacheManagerBuilder = cacheManagerBuilder.withCache(USER_CACHE_NAME, userCacheConfigurationBuilder);
		userCacheManager = cacheManagerBuilder.build(true);

		userObjectCache = userCacheManager.getCache(USER_CACHE_NAME, String.class, UserObject.class);

		// identifierMap can be modified concurrently by different requests
		identifierMap = new ConcurrentHashMap<String, String>();
	}
	
	/**
	 * Parsing the setting and convert it to Duration
	 * @param timeToLive
	 * @return timetolive in Duration type
	 */
	private Duration getTimeToLive(String timeToLive) {
		
		Duration duration = null;

		if (timeToLive.equals("INFINITE")) {
			duration = Duration.INFINITE;
			LOG.info("Setting time to live to INFINITE");
		} else {

			TimeUnit unit = TimeUnit.DAYS;
			int iTimeToLive = 1;

			String[] temp = timeToLive.split("_");

			try {
				iTimeToLive = Integer.parseInt(temp[0]);
			} catch (IllegalArgumentException e) {
				LOG.error("Invalid time_to_live value(s), resetting to 1_DAYS");
				iTimeToLive = 1;
			}

			try {
				switch (temp[1]) {
				case "SECS":
					unit = TimeUnit.SECONDS;
					break;
				case "MINS":
					unit = TimeUnit.MINUTES;
					break;
				case "HRS":
					unit = TimeUnit.HOURS;
					break;
				case "DAYS":
					unit = TimeUnit.DAYS;
					break;
				default:
				}
			} catch (Exception ex) {
				LOG.error("Invalid time_to_live unit, resetting to DAYS");
				unit = TimeUnit.DAYS;
			}

			duration = Duration.of(iTimeToLive, unit);

			LOG.info(String.format("Setting expiration to %d %s", iTimeToLive, unit.toString()));
			
		}
		return duration;
	}

	/**
	 * Retrieve User object from cache
	 * @param id Identifier of the user object
	 * @return Object of UserObject
	 */
	public UserObject getUserObjectFromCache(String id) {
		if (userObjectCache == null) {
			LOG.error("User Cache has not been initialized");
			return null;
		}

		return (identifierMap.get(id) == null) ? null : userObjectCache.get(identifierMap.get(id));
	}
	
	
	/**
	 * Printout object in cache
	 * 
	 */
	public void printCache() {
		
		//User Cache
		if (userObjectCache == null) {
			LOG.error("User Cache has not been initialized");
			return;
		}

		int size = 0;

		Iterator<Entry<String, UserObject>> it = userObjectCache.iterator();
		while (it.hasNext()) {
			Entry<String, UserObject> entry = (Entry<String, UserObject>) it.next();
			size++;
			LOG.info(String.format("User Cache now contains [%s]", entry.getKey()));
		}

		LOG.info(String.format("User Cache now has [%d] entries", size));
		
	}
	
	/**
	 * Print out User object identifier map
	 * 
	 */
	public void printIdentifierMap() {
		for (Map.Entry<String, String> entry : identifierMap.entrySet()) {
			LOG.info(String.format("Identifier map now contains [%s - %s]", entry.getKey(), entry.getValue()));
		}
	}
	
	/**
	 * Writing identifier into identifier map
	 * @param id Identifier ID
	 * @param combinedId Combination of ID
	 */
	public void addIdentifier(String id, String combinedId) {
		if (identifierMap == null) {
			LOG.error("Cache has not been initialized");
			return;
		}
		identifierMap.put(id, combinedId);
	}
}
