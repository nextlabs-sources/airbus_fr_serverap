package com.nextlabs.common;

/**
 * Abstract class for Profile
 * 
 * @author klee
 *
 */
public abstract class Profile {
	protected String name;
	
	
	/**
	 * Constructor for profile
	 * @param name Profile name
	 */
	public Profile(String name) {
		this.name = name;
	}

	/**
	 * Get the name of profile
	 * @return Name of the Profile
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set the name of the profile
	 * @param name Profile name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
}
