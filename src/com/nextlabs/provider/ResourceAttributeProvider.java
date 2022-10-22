package com.nextlabs.provider;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.IEvalValue;
import com.bluejungle.framework.expressions.IMultivalue;
import com.bluejungle.pf.domain.destiny.serviceprovider.IResourceAttributeProvider;
import com.bluejungle.pf.domain.destiny.serviceprovider.ServiceProviderException;
import com.bluejungle.pf.domain.epicenter.resource.IResource;
import com.nextlabs.cache.ResourceCacheEngine;
import com.nextlabs.common.Constants;
import com.nextlabs.common.PropertyLoader;
import com.nextlabs.common.ResourceObject;
import com.nextlabs.common.Util;
import com.nextlabs.db.DBResouceProvider;
import com.nextlabs.task.ResourcesRefreshTask;

/**
 * This class implement the entry point for handling request from PDP, PEP will call into getAttribute and retrieve the value needed.
 * 
 * @author klee
 * 
 */
public class ResourceAttributeProvider implements IResourceAttributeProvider {
	private static final Log LOG = LogFactory.getLog(ResourceAttributeProvider.class);
	private Properties PLUGIN_PROPS;
	private final String CLIENT_PROPS_FILE = "jservice/config/ResourceReferentialPlugin.properties";
	private IEvalValue nullReturn;
	ResourceCacheEngine engine;
	DBResouceProvider dbProvider;
	private static String LOG_INCOMING_REUQEST = "Incoming request from PEP with resource ID [%s] and request attribute name [%s] with RTYPE [%s]";
	private static String LOG_RES_CACHE_MISSED = "Cache missed for resource id  [%s]. Attempt to query...";
	private static String LOG_RES_ATTRIBUTE_NEEDED = "Attribute [%s] is needed from Resources DB table";
	private static String LOG_TIME_TAKEN = "Time Taken: %sms";


	/* (non-Javadoc)
	 * @see com.bluejungle.pf.domain.destiny.serviceprovider.IServiceProvider#init()
	 * Initialize all the needed resources for the plugin
	 */
	public void init() {
		long startTime = System.nanoTime();
		LOG.debug("init() started");
		PLUGIN_PROPS = PropertyLoader.loadPropertiesInPDP(CLIENT_PROPS_FILE);

		// Set null return
		String nullString = PLUGIN_PROPS.getProperty("null_string");
		if (nullString == null) {
			nullReturn = EvalValue.NULL;
		} else {
			nullReturn = EvalValue.build(nullString);
		}

		// Initialize Cache
		engine = ResourceCacheEngine.getInstance();
		engine.initializeCache(PLUGIN_PROPS);

		dbProvider = DBResouceProvider.getInstance();
		dbProvider.setCommonProperties(PLUGIN_PROPS);

		if (PLUGIN_PROPS.getProperty("profile_names") == null
				|| PLUGIN_PROPS.getProperty("profile_names").length() == 0) {
			dbProvider.setIsSingleProfile(true);
			dbProvider.loadSingleProfile(PLUGIN_PROPS, PropertyLoader.getPropertiesFilePath(CLIENT_PROPS_FILE));
		} else {
			dbProvider.setIsSingleProfile(false);
			dbProvider.loadProfiles(PLUGIN_PROPS, PropertyLoader.getPropertiesFilePath(CLIENT_PROPS_FILE));
		}

		try {	
			dbProvider.initDBConnetionPool(dbProvider.getProfile());

			if (PLUGIN_PROPS.getProperty("expired_mode", "purge").equals("purge")) {
				LOG.info("Schedule timer for purging resources cache");

				scheduleTimer();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

		LOG.info("init() completed successfully");
		LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
	}

	/**
	 * Schedule the timer for reload the cache
	 */
	private void scheduleTimer() {

		String pattern = "HH:mm";

		String formattedDate = PLUGIN_PROPS.getProperty("purge_time");

		Date scheduleTime = null;

		Calendar scheduleCal = Calendar.getInstance();

		if (formattedDate == null) {
			LOG.warn("aor_purge_time is not set. Cache refresh process will be started immediately");
		} else {
			try {
				scheduleTime = new SimpleDateFormat(pattern).parse(formattedDate);
				scheduleCal.setTime(scheduleTime);
			} catch (Exception e) {
				LOG.error("Cannot parse aor_purge_time. Cache refresh process will be started immediately", e);
			}
		}

		String refreshPeriodString = "1_DAYS";

		TimeUnit unit = TimeUnit.MILLISECONDS;
		int refreshPeriod;

		String[] temp = refreshPeriodString.split("_");

		try {
			refreshPeriod = Integer.parseInt(temp[0]);
		} catch (IllegalArgumentException e) {
			LOG.error("Invalid cache_refresh_period value(s), resetting to 1_DAYS", e);
			refreshPeriod = 1000 * 60 * 60 * 24;
		}

		try {
			switch (temp[1]) {
			case "SECS":
				refreshPeriod = refreshPeriod * 1000;
				break;
			case "MINS":
				refreshPeriod = refreshPeriod * 1000 * 60;
				break;
			case "HRS":
				refreshPeriod = refreshPeriod * 1000 * 60 * 60;
				break;
			case "DAYS":
				refreshPeriod = refreshPeriod * 1000 * 60 * 60 * 24;
				break;
			default:
			}
		} catch (Exception ex) {
			LOG.error("Invalid cache_refresh_period unit, resetting to DAYS", ex);
			refreshPeriod = refreshPeriod * 1000 * 60 * 60 * 24;
		}

		if (scheduleTime != null) {
			LOG.info(String.format("Cache refresh period is set as [%s]", Util.getDurationBreakdown(refreshPeriod)));
		}

		Long startTime = 0L;

		if (LocalDateTime.now().getHour() <= scheduleCal.get(Calendar.HOUR_OF_DAY)) {
			startTime = LocalDateTime.now().until(LocalDate.now().plusDays(0)
					.atTime(scheduleCal.get(Calendar.HOUR_OF_DAY), scheduleCal.get(Calendar.MINUTE)), ChronoUnit.MILLIS)
					+ System.currentTimeMillis();
		} else {
			startTime = LocalDateTime.now().until(LocalDate.now().plusDays(1)
					.atTime(scheduleCal.get(Calendar.HOUR_OF_DAY), scheduleCal.get(Calendar.MINUTE)), ChronoUnit.MILLIS)
					+ System.currentTimeMillis();
			;
		}

		LOG.info(String.format("Cache refresh process should start after [%s]",
				(startTime == null || startTime - System.currentTimeMillis() < 0) ? (0 + " minute")
						: Util.getDurationBreakdown(startTime - System.currentTimeMillis())));

		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		service.scheduleAtFixedRate(ResourcesRefreshTask.getInstance(),
				(startTime == null) ? 0 : (startTime - System.currentTimeMillis()), refreshPeriod, unit);
		LOG.info("Cache refresh process has been scheduled");
	}


	/* (non-Javadoc)
	 * @see com.bluejungle.pf.domain.destiny.serviceprovider.IResourceAttributeProvider#getAttribute(com.bluejungle.pf.domain.epicenter.resource.IResource, java.lang.String)
	 * Main entry where the PDP will call in to get the attribute needed for a resource
	 */
	public synchronized IEvalValue getAttribute(IResource resource, String attribute) throws ServiceProviderException {

		try {
			long startTime = System.nanoTime();
			String resID = (String) resource.getAttribute(Constants.RESOURCE_ID).getValue();
			String rType = (String) resource.getAttribute(Constants.RTYPE).getValue();
			LOG.info(String.format(LOG_INCOMING_REUQEST, resID, attribute, rType));

			LOG.debug(String.format("Getting attribute [%s] for [%s]", attribute.toLowerCase(), resID));

			if (rType == null) {
				// Handling for Provider refreshing cache.
				while (dbProvider.isRefreshing()) {
					LOG.info("Resource Provider is flusing cache, sleep for 20ms then re-try");
					try {
						Thread.sleep(20);
					} catch (InterruptedException e) {
						LOG.error(e.getMessage(), e);
					}
				}

				// Determine attribute from where
				if (dbProvider.getProfile().getAttributesToPull().contains(attribute)
						|| dbProvider.getProfile().getTableProgHDAttributesToPull().contains(attribute)
						|| dbProvider.getProfile().getTableProgITAttributesToPull().contains(attribute)
						|| dbProvider.getProfile().getTableEXCC2TAttributesToPull().contains(attribute)) {

					ResourceObject resObj = engine.getItemObjectFromCache(resID);

					// try again with case insensitive
					if (resObj == null) {
						resObj = engine.getItemObjectFromCache(resID.toLowerCase());
					}

					// cache doesn't contain the user, query from DB
					if (resObj == null) {
						LOG.info(String.format(LOG_RES_CACHE_MISSED, resID));

						try {
							resObj = dbProvider.getItemObject(resID, attribute.toLowerCase());
						} catch (Exception e) {
							LOG.error(String.format("Unable to query for resource with id [%s]", resID));
							LOG.error(e.getMessage(), e);
							return nullReturn;
						}
					}

					if (resObj == null) {
						LOG.warn(String.format("Cannot resolve attribute [%s] for [%s] after query resource DB",
								attribute, resID));
						LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
						return nullReturn;
					}

					LOG.info(String.format(LOG_RES_ATTRIBUTE_NEEDED, attribute));
					
					// If attribute request is ecc2 and ectype = part then switch the attribute to
					// ecc2_1
					if (attribute.equalsIgnoreCase(Constants.ATTRIBUTE_EXCC2)) {
						IEvalValue ecTypeEval = resource.getAttribute(Constants.ATTRIBUTE_ECTYPE);

						if (ecTypeEval != IEvalValue.NULL && ecTypeEval.getValue().toString().equalsIgnoreCase(Constants.ATTRIBUTE_ECTYPE_PART)) {
							LOG.info("ECType is part, switching attribute to look from MAEX table");
							attribute = Constants.ATTRIBUTE_EXCC2_1;
						}
					}

					IEvalValue val = resObj.getAttribute(attribute.toLowerCase());

					if (val == null || val.getValue() == null) {
						LOG.info(String.format("Attribute [%s] is null for resource [%s]", attribute, resID));
						val = nullReturn;
					}

					if (val.getValue() instanceof IMultivalue) {
						StringBuilder sb = new StringBuilder("[").append(resID).append("] has attribute [").append(attribute).append("] with value =");

						boolean first = true;
						for (IEvalValue v : (IMultivalue) val.getValue()) {
							if (!first) {
								sb.append(", ");
							}
							first = false;
							if (v == null) {
								sb.append("null");
							} else {
								sb.append(v.getValue());
							}
						}
												
						if (first) {
							sb.append("[MULTI_EMPTY]");
						}
						
						LOG.info(sb.toString());
					} else {
						LOG.info(String.format("resource id [%s] has attribute [%s] with value = [%s]", resID,attribute, val.getValue()));
					}
					LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
					
					return val;

				} else {
					LOG.info(String.format("Unknow attribute [%s] request from PEP, will return Java null",attribute));
					LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
					return null;
				}

			} else if (rType.equalsIgnoreCase(Constants.RTYPE_PROGRAM)) {

				// Determine attribute from where
				if (dbProvider.getProfile().getProgAttributesToPull().contains(attribute)) {

					if (!resID.matches("-?(0|[1-9]\\d*)")) {
						LOG.error(
								"Resource ID is not integer value, skip getting attribute value and return empty value");
						return null;
					}

					ResourceObject resObj = engine.getProgObjectFromCache(resID);

					// try again with case insensitive
					if (resObj == null) {
						resObj = engine.getProgObjectFromCache(resID.toLowerCase());
					}

					// cache doesn't contain the user, query from AD
					if (resObj == null) {
						LOG.info(String.format(LOG_RES_CACHE_MISSED, resID));

						try {
							resObj = dbProvider.getProgramObject(resID, attribute.toLowerCase());
						} catch (Exception e) {
							LOG.error(String.format("Unable to query for resource with id [%s]", resID));
							LOG.error(e.getMessage(), e);
							return nullReturn;
						}
					}

					if (resObj == null) {
						LOG.warn(String.format("Cannot resolve attribute [%s] for [%s] after query resource DB",
								attribute, resID));
						LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
						return nullReturn;
					}

					LOG.info(String.format(LOG_RES_ATTRIBUTE_NEEDED, attribute));

					IEvalValue val = resObj.getAttribute(attribute.toLowerCase());

					if (val == null || val.getValue() == null) {
						LOG.info(String.format("Attribute [%s] is null for resource [%s]", attribute, resID));
						val = nullReturn;
					}

					if (val.getValue() instanceof IMultivalue) {
						StringBuilder sb = new StringBuilder("[").append(resID).append("] has attribute [").append(attribute).append("] with value = ");

						boolean first = true;
						for (IEvalValue v : (IMultivalue) val.getValue()) {
							if (!first) {
								sb.append(", ");
							}
							first = false;
							if (v == null) {
								sb.append("null");
							} else {
								sb.append(v.getValue());
							}
							
							if (first) {
								sb.append("[MULTI_EMPTY]");
							}
						}
						LOG.info(sb.toString());
					} else {
						LOG.info(String.format("resource id [%s] has attribute [%s] with value = [%s]", resID,
								attribute, val.getValue()));
					}
					LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));

					return val;

				} else {

					LOG.info(String.format("Unknow attribute [%s] request from PEP, will return Java null", attribute));
					LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
					return null;
				}

			} else {
				
				LOG.info(String.format("Unknow RTYPE [%s] request from PEP, will return Java null", rType));
				LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
				return null;
			}

		} catch (Exception e) {
			LOG.error("Fatal exception occured, returning IEValve null value");
			LOG.error(e.getMessage(), e);
			return nullReturn;
		}
		
	}

	/**
	 * Compute the time different between start and stop
	 * @param start Start time in milli
	 * @param end Stop time in milli
	 * @return Different between start and stop time  in ms
	 */
	private String computeTimeTaken(long start, long end) {
		double differenceInMilli = (end - start) / 1000000.00;
		return Double.toString(differenceInMilli);
	}

}
