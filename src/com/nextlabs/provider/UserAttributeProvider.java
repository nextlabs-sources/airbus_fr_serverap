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
import com.bluejungle.pf.domain.destiny.serviceprovider.ISubjectAttributeProvider;
import com.bluejungle.pf.domain.destiny.serviceprovider.ServiceProviderException;
import com.bluejungle.pf.domain.destiny.subject.IDSubject;
import com.nextlabs.cache.UserCacheEngine;
import com.nextlabs.common.PropertyLoader;
import com.nextlabs.common.UserObject;
import com.nextlabs.common.Util;
import com.nextlabs.db.DBUserProvider;
import com.nextlabs.task.SubjectRefreshTask;

/**
 * This class implement the entry point for handling request from PDP, PEP will call into getAttribute and retrieve the value needed.
 * 
 * @author klee
 *
 */
public class UserAttributeProvider implements ISubjectAttributeProvider {
	private static final Log LOG = LogFactory.getLog(UserAttributeProvider.class);
	private Properties PLUGIN_PROPS;
	private final String CLIENT_PROPS_FILE = "jservice/config/UserReferentialPlugin.properties";
	private IEvalValue nullReturn;
	UserCacheEngine engine;
	DBUserProvider dbUserProvider;
	private static String LOG_INCOMING_REUQEST = "Incoming request from PEP with userId [%s]";
	private static String LOG_USER_CACHE_MISSED = "Cache missed for USER [%s]. Attempt to query...";
	private static String LOG_USER_ATTRIBUTE_NEEDED = "Attribute [%s] is needed from User DB";
	private static String LOG_LINK_ATTRIBUTE_NEEDED = "Attribute [%s] is needed from link attribute";
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
		engine = UserCacheEngine.getInstance();
		engine.initializeCache(PLUGIN_PROPS);

		dbUserProvider = DBUserProvider.getInstance();
		dbUserProvider.setCommonProperties(PLUGIN_PROPS);
		
		if (PLUGIN_PROPS.getProperty("profile_names") == null|| PLUGIN_PROPS.getProperty("profile_names").length() == 0) {
			dbUserProvider.setIsSingleProfile(true);
			dbUserProvider.loadSingleProfile(PLUGIN_PROPS, PropertyLoader.getPropertiesFilePath(CLIENT_PROPS_FILE));
		} else {
			dbUserProvider.setIsSingleProfile(false);
			dbUserProvider.loadProfiles(PLUGIN_PROPS, PropertyLoader.getPropertiesFilePath(CLIENT_PROPS_FILE));
		}
		
		try {
			
			dbUserProvider.initDBConnetionPool(dbUserProvider.getProfile());
			
			if(PLUGIN_PROPS.getProperty("expired_mode","purge").equals("purge")) {
				LOG.info("Schedule timer for purging user cache");
				//dbUserProvider.refreshCache();
				scheduleTimer();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

		LOG.info("init() completed successfully");
		LOG.info(String.format(LOG_TIME_TAKEN,computeTimeTaken(startTime, System.nanoTime())));
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
			LOG.warn("purge_time is not set. Cache refresh process will be started immediately");
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
			startTime=LocalDateTime.now().until(LocalDate.now().plusDays(0).atTime(scheduleCal.get(Calendar.HOUR_OF_DAY), scheduleCal.get(Calendar.MINUTE)), ChronoUnit.MILLIS) + System.currentTimeMillis();
		}
		else {
			startTime=LocalDateTime.now().until(LocalDate.now().plusDays(1).atTime(scheduleCal.get(Calendar.HOUR_OF_DAY), scheduleCal.get(Calendar.MINUTE)), ChronoUnit.MILLIS) + System.currentTimeMillis();;
		}
		
		LOG.info(String.format("Cache refresh process should start after [%s]",(startTime == null || startTime - System.currentTimeMillis() < 0) ? (0 + " minute")
						: Util.getDurationBreakdown(startTime - System.currentTimeMillis())));

		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		service.scheduleAtFixedRate(SubjectRefreshTask.getInstance(),(startTime == null) ? 0 : (startTime - System.currentTimeMillis()), refreshPeriod, unit);
		LOG.info("Cache refresh process has been scheduled");
	}

	/* (non-Javadoc)
	 * @see com.bluejungle.pf.domain.destiny.serviceprovider.ISubjectAttributeProvider#getAttribute(com.bluejungle.pf.domain.destiny.subject.IDSubject, java.lang.String)
	 * Main entry where the PDP will call in to get the attribute needed for a subject
	 */
	public synchronized IEvalValue getAttribute(IDSubject subj, String attribute) throws ServiceProviderException {

		try {

			long startTime = System.nanoTime();
			String userId = subj.getUid();

			LOG.info(String.format(LOG_INCOMING_REUQEST, userId));

			LOG.debug(String.format("Getting attribute [%s] for [%s]", attribute.toLowerCase(), userId));

			if (dbUserProvider.getProfile().getAttributesToPull().contains(attribute)
					|| dbUserProvider.getProfile().getLinkAttributesToPull().contains(attribute)) {

				// Handling for Provider refreshing cache.
				while (dbUserProvider.isRefreshing()) {
					LOG.info("DB User Provider is flusing user cache, sleep for 20ms then re-try");
					try {
						Thread.sleep(20);
					} catch (InterruptedException e) {
						LOG.error(e.getMessage(), e);
					}

				}
				UserObject userObj = engine.getUserObjectFromCache(userId);

				// try again with case insensitive
				if (userObj == null) {
					userObj = engine.getUserObjectFromCache(userId.toLowerCase());
				}

				// cache doesn't contain the user, query from AD
				if (userObj == null) {
					LOG.info(String.format(LOG_USER_CACHE_MISSED, userId));

					try {
						userObj = dbUserProvider.getUserObject(userId, attribute.toLowerCase());
					} catch (Exception e) {
						LOG.error(String.format("Unable to query for USER [%s]", userId));
						LOG.error(e.getMessage(), e);
						return nullReturn;
					}
				}
				if (userObj == null) {
					LOG.warn(String.format("Cannot resolve attribute [%s] for [%s] after query DB", attribute, userId));
					LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
					return nullReturn;
				}

				// Determine attribute from where
				if (dbUserProvider.getProfile().getAttributesToPull().contains(attribute)) {
					LOG.info(String.format(LOG_USER_ATTRIBUTE_NEEDED, attribute));
				} else if (dbUserProvider.getProfile().getLinkAttributesToPull().contains(attribute)) {
					LOG.info(String.format(LOG_LINK_ATTRIBUTE_NEEDED, attribute));
				}

				IEvalValue val = userObj.getAttribute(attribute.toLowerCase());

				if (val == null || val.getValue() == null) {
					LOG.info(String.format("Attribute [%s] is null for user [%s]", attribute, userId));
					val = nullReturn;
				}

				if (val.getValue() instanceof IMultivalue) {
					StringBuilder sb = new StringBuilder("[").append(userId).append("] has attribute [")
							.append(attribute).append("] with value = ");

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
					LOG.debug(sb.toString());
				} else {
					LOG.info(String.format("user [%s] has attribute [%s] with value = [%s]", userId, attribute, val.getValue()));
				}
				LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));

				return val;

			} else {
				LOG.info(String.format("Unknow attribute [%s] request from PEP, will return Java NULL", attribute));
				LOG.info(String.format(LOG_TIME_TAKEN, computeTimeTaken(startTime, System.nanoTime())));
				return null;
			}

		} catch (Exception e) {
			LOG.error("Fatal exception occured, returning PDP null value");
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
	private String computeTimeTaken(long start, long end){
		double differenceInMilli = (end - start) / 1000000.00;
		return Double.toString(differenceInMilli);
	}

}
