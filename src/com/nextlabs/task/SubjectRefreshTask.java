package com.nextlabs.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nextlabs.db.DBUserProvider;

/**
 * This class provide thread execution for running reload task for subject or user
 * 
 * @author klee
 * 
 */
public class SubjectRefreshTask implements Runnable {

	private static final Log LOG = LogFactory.getLog(SubjectRefreshTask.class);

	private static SubjectRefreshTask task;

	/**
	 * Singleton control of the reload task
	 * @return Object of SubjectRefreshTask
	 */
	public static SubjectRefreshTask getInstance() {
		if (task == null) {
			task = new SubjectRefreshTask();
		}

		return task;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override	
	public void run() {
		LOG.info("Cache refresh started");
		
		DBUserProvider.getInstance().refreshCache();
		
		LOG.info("Cache refresh finished");

	}

}
