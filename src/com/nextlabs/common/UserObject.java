package com.nextlabs.common;

import java.util.HashMap;
import java.util.Map;

import com.bluejungle.framework.expressions.IEvalValue;

/**
 * Define the user object and is a place holder for user and all the attributes for the user.
 * @author klee
 *
 */
public class UserObject {
	private String id;
	private Map<String, IEvalValue> attributes;
	private String domain;
	private String type;

	/**
	 * @param domain Profile or domain name
	 * @param id ID of the user
	 * @param type Type of object
	 */
	public UserObject(String domain, String id, String type) {
		this.id = id;
		this.domain = domain;
		attributes = new HashMap<String, IEvalValue>();
		this.type = type;
	}
	
	/**
	 * Add the attribute name and value to the object
	 * @param key Attribute name 
	 * @param value Attribute Value
	 */
	public void addAttribute(String key, IEvalValue value) {
		attributes.put(key, value);
	}
	
	/**
	 * Retrieve the object for given attribute name
	 * @param key Attribute name
	 * @return IEvalValue object for given attribute name
	 */
	public IEvalValue getAttribute(String key) {
		return attributes.get(key);
	}

	/**
	 * Retrieve ID of the user
	 * @return ID of the user
	 */
	public String getId() {
		return id;
	}

	/**
	 * Set the ID for the user
	 * @param id ID of the user
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Retrieve all the attributes for the object
	 * @return Map contain attribute name and IEvalValue object
	 */
	public Map<String, IEvalValue> getAttributes() {
		return attributes;
	}

	/**
	 * Store the attributes list with IEvalValue object into the user object
	 * @param attributes Map contain attribute name and IEvalValue object
	 */
	public void setAttributes(Map<String, IEvalValue> attributes) {
		this.attributes = attributes;
	}

	/**
	 * Retrieve profile or domain name
	 * @return Profile or Domain name
	 */
	public String getDomain() {
		return domain;
	}

	/**
	 * Set profile or domain name
	 * @param domain  Profile or Domain name
	 */
	public void setDomain(String domain) {
		this.domain = domain;
	}

	/**
	 * Retrieve type of the object
	 * @return Type of the object
	 */
	public String getType() {
		return type;
	}

	/**
	 * Store type of the object
	 * @param type Type of the object
	 */
	public void setType(String type) {
		this.type = type;
	}

}
