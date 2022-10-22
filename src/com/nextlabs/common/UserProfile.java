package com.nextlabs.common;

import java.util.Set;

/**
 * Abstract class that extends from Profile and define the method used for user
 * 
 * @author klee
 *
 */
public abstract class UserProfile extends Profile {

	/**
	 * Instantiate the User profile
	 * @param name Profile Name
	 */
	public UserProfile(String name) {
		super(name);
	}
	
	/**
	 * Retrieve list of attribute to be pull from data source
	 * @return list of attributes to retrieve from the main table
	 */
	public Set<String> getAttributesToPull() {
		return null;
	}
	
	/**
	 * Retrieve list of attribute to be pull from data source
	 * @return list of attributes to retrieve from the secondary table
	 */
	public Set<String> getLinkAttributesToPull() {	
		return null;
	}

}
