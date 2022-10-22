package com.nextlabs.common;

import java.util.Set;

/**
 * Abstract class that extends from Profile and define the method used for resource
 * 
 * @author klee
 *
 */
public abstract class ResourceProfile extends Profile {

	/**
	 * Instantiate the Resource profile
	 * @param name Profile Name
	 */
	public ResourceProfile(String name) {
		super(name);
	}

	/**
	 * Retrieve the list of attributes to pull from ProgHD table 
	 * @return list of attributes
	 */
	public abstract Set<String> getTableProgHDAttributesToPull();

	/**
	 * Retrieve the list of attributes to pull from PROGRAM table 
	 * @return list of attributes
	 */
	public abstract Set<String> getProgAttributesToPull();
	
	/**
	 * Retrieve the list of attributes to pull from ProgIT table 
	 * @return list of attributes
	 */
	public abstract Set<String> getTableProgITAttributesToPull();

	/**
	 * Retrieve the list of attributes to pull from EXCC2T table 
	 * @return list of attributes
	 */
	public abstract Set<String> getTableEXCC2TAttributesToPull();

}
