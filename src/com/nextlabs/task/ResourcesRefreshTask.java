package com.nextlabs.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nextlabs.db.DBResouceProvider;

/**
 * This class provide thread execution for running reload task for resource
 * 
 * @author klee
 * 
 */
public class ResourcesRefreshTask implements Runnable {

	private static final Log LOG = LogFactory.getLog(ResourcesRefreshTask.class);

	private static ResourcesRefreshTask task;

	/**
	 * Singleton control of the reload task
	 * @return Object of ResourcesRefreshTask
	 */
	public static ResourcesRefreshTask getInstance() {
		if (task == null) {
			task = new ResourcesRefreshTask();
		}

		return task;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override	
	public void run() {
		LOG.info("Cache refresh started");
		
		DBResouceProvider.getInstance().refreshCache();
		
		LOG.info("Cache refresh finished");

	}

}
