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

import com.nextlabs.common.ResourceObject;


/**
 * This class implement the CacheEngine for storing and retrieving item and program.
 * 
 * @author klee
 *
 */
public class ResourceCacheEngine {

	public static final String ITEM_CACHE_NAME = "ItemCache";
	public static final String PROGRAM_CACHE_NAME = "ProgCache";
	private static final Log LOG = LogFactory.getLog(ResourceCacheEngine.class);
	private static ResourceCacheEngine engine;
	private CacheManager itemCacheManager;
	private CacheManager progCacheManager;
	private Cache<String, ResourceObject> itemObjectCache;
	private Cache<String, ResourceObject> progObjectCache;

	private Map<String, String> itemIdentifierMap;
	private Map<String, String> progIdentifierMap;

	public ResourceCacheEngine() {
	}
	
	/**
	 * Singleton method using getInstance
	 * @return Object of ResourceCacheEngine
	 */
	public static ResourceCacheEngine getInstance() {
		if (engine == null) {
			engine = new ResourceCacheEngine();
		}
		return engine;
	}
	
	/**
	 * Storing ResourceObject into Item Cache region
	 * @param obj ResourceObject to put into cache
	 */
	public void writeObjectToItemCache(ResourceObject obj) {
		if (itemObjectCache == null) {
			LOG.error("Item Cache has not been initialized");
			return;
		}
		itemObjectCache.put(obj.getId(), obj);
	}
	

	/**
	 * Storing ResourceObject into Program cache region
	 * @param obj ResourceObject to put into cache
	 */
	public void writeObjectToProgramCache(ResourceObject obj) {
		if (progObjectCache == null) {
			LOG.error("Program Cache has not been initialized");
			return;
		}
		progObjectCache.put(obj.getId(), obj);
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
		
		//Item Cache region
		CacheConfigurationBuilder<String, ResourceObject> itemCacheConfigurationBuilder = CacheConfigurationBuilder
				.newCacheConfigurationBuilder(String.class, ResourceObject.class, resourceBuilder)
				.withExpiry(Expirations.timeToLiveExpiration(duration)).withSizeOfMaxObjectGraph(lCache_max_object);

		CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
		cacheManagerBuilder = cacheManagerBuilder.withCache(ITEM_CACHE_NAME, itemCacheConfigurationBuilder);
		itemCacheManager = cacheManagerBuilder.build(true);

		itemObjectCache = itemCacheManager.getCache(ITEM_CACHE_NAME, String.class, ResourceObject.class);
		
		// Program Cache region	
		CacheConfigurationBuilder<String, ResourceObject> progCacheConfigurationBuilder = CacheConfigurationBuilder
				.newCacheConfigurationBuilder(String.class, ResourceObject.class, resourceBuilder)
				.withExpiry(Expirations.timeToLiveExpiration(duration)).withSizeOfMaxObjectGraph(lCache_max_object);

		CacheManagerBuilder<CacheManager> progCacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
		progCacheManagerBuilder = progCacheManagerBuilder.withCache(PROGRAM_CACHE_NAME, progCacheConfigurationBuilder);
		progCacheManager = progCacheManagerBuilder.build(true);

		progObjectCache = progCacheManager.getCache(PROGRAM_CACHE_NAME, String.class, ResourceObject.class);
		
		// identifierMap can be modified concurrently by different requests
		itemIdentifierMap = new ConcurrentHashMap<String, String>();
		progIdentifierMap = new ConcurrentHashMap<String, String>();
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
	 * Retrieve Item object from cache
	 * @param id Identifier of the item object
	 * @return Object of ResourceObject
	 */
	public ResourceObject getItemObjectFromCache(String id) {
		if (itemObjectCache == null) {
			LOG.error("Item Cache has not been initialized");
			return null;
		}

		return (itemIdentifierMap.get(id) == null) ? null : itemObjectCache.get(itemIdentifierMap.get(id));
	}
	
	
	/**
	 * Retrieve Item object from cache
	 * @param id Identifier of the item object
	 * @return Object of ResourceObject
	 */
	public ResourceObject getProgObjectFromCache(String id) {
		
		if (progObjectCache == null) {
			LOG.error("Prog Cache has not been initialized");
			return null;
		}

		return (progIdentifierMap.get(id) == null) ? null : progObjectCache.get(progIdentifierMap.get(id));
	}
	
	
	/**
	 * Printout object in cache
	 * 
	 */
	public void printItemCache() {
		
		//Item Cache
		if (itemObjectCache == null) {
			LOG.error("Item Cache has not been initialized");
			return;
		}

		int size = 0;

		Iterator<Entry<String, ResourceObject>> it = itemObjectCache.iterator();
		while (it.hasNext()) {
			Entry<String, ResourceObject> entry = it.next();
			size++;
			LOG.info(String.format("User Cache now contains [%s]", entry.getKey()));
		}

		LOG.info(String.format("Item Cache now has [%d] entries", size));
		
	}
	
	/**
	 * Printout program object in cache
	 * 
	 */
	public void printProgCache() {
		
		//Item Cache
		if (progObjectCache == null) {
			LOG.error("Program Cache has not been initialized");
			return;
		}

		int size = 0;

		Iterator<Entry<String, ResourceObject>> it = progObjectCache.iterator();
		while (it.hasNext()) {
			Entry<String, ResourceObject> entry = it.next();
			size++;
			LOG.info(String.format("Prog Cache now contains [%s]", entry.getKey()));
		}

		LOG.info(String.format("Prog Cache now has [%d] entries", size));
		
	}

	
	/**
	 * Print out Item object identifier map
	 * 
	 */
	public void printItemIdentifierMap() {
		for (Map.Entry<String, String> entry : itemIdentifierMap.entrySet()) {
			LOG.info(String.format("Item Identifier map now contains [%s - %s]", entry.getKey(), entry.getValue()));
		}
	}
	
	/**
	 * Printout Program object identifier map
	 */
	public void printProgIdentifierMap() {
		for (Map.Entry<String, String> entry : progIdentifierMap.entrySet()) {
			LOG.info(String.format("Prog Identifier map now contains [%s - %s]", entry.getKey(), entry.getValue()));
		}
	}

	
	/**
	 * Writing identifier into Item identifier map
	 * @param id Identifier ID
	 * @param combinedId Combination of ID
	 */
	public void addItemIdentifier(String id, String combinedId) {
		if (itemIdentifierMap == null) {
			LOG.error("Cache has not been initialized");
			return;
		}
		itemIdentifierMap.put(id, combinedId);
	}
	
	/**
	 * Writing identifier into Program identifier map
	 * @param id Identifier ID
	 * @param combinedId Combination of ID
	 */
	public void addProgIdentifier(String id, String combinedId) {
		if (progIdentifierMap == null) {
			LOG.error("Cache has not been initialized");
			return;
		}
		progIdentifierMap.put(id, combinedId);
	}
}
