package com.nextlabs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.Multivalue;
import com.nextlabs.cache.UserCacheEngine;
import com.nextlabs.common.Provider;
import com.nextlabs.common.ResourceObject;
import com.nextlabs.common.UserObject;
import com.nextlabs.common.Util;
import com.nextlabs.exception.InvalidProfileException;
	
/**
 * Class the implement the detail for fetching data from user database
 * 
 * @author klee
 *
 */
public class DBUserProvider implements Provider {	
	private static final Log LOG = LogFactory.getLog(DBUserProvider.class);
	private static DBUserProvider provider;
	private Map<String, UserDBProfile> profiles;
	private UserDBProfile singleProfile;
	private Map<String, List<String>> userAttributeToProfileMap;
	private boolean isSingleProfile;
	private Map<String, String> idToObjectTypeMap;
	private final String USER_TYPE = "user";
	private int numberOfRetries;
	private int intervalBetweenRetries;
	private Boolean isRefreshing;
	private static Properties commonProp;
	private static HikariCPDataSource ds;
	private static String mainSQLQuery = "SELECT A.ID , B.LOGID, ## FROM USRPD A LEFT join USRAT_APLO B ON A.ID = B.ID_USRPD";
	private static String singleUserSQLQuery = "SELECT A.ID, B.LOGID, ## FROM USRPD A LEFT join USRAT_APLO B ON A.ID = B.ID_USRPD  WHERE LogonID = ? OR LogID = ?";
	private static final String SQL_LINK_QUERY = "SELECT PROGK, PROGN, PROVS FROM PROGHD A INNER JOIN USRAT_PROGT B ON A.ID_HD = B.ID_HD WHERE B.ID = ?";

	/**
	 * Constructor for DBUserProvider
	 */
	public DBUserProvider() {
		userAttributeToProfileMap = new HashMap<String, List<String>>();
		idToObjectTypeMap = new ConcurrentHashMap<String, String>();
		isRefreshing = false;
	}

	/**
	 * Singleton control of the DBResourceProvider
	 * @return Object of DBUserProvider
	 */
	public static DBUserProvider getInstance() {
		if (provider == null) {
			provider = new DBUserProvider();
		}

		return provider;
	}
	
	
	/**
	 * Construct the column for query
	 * @param profile UserDBProfile which contain all the profile information
	 * @return String contain the column user for query for e.g "[Nationality], [DOB]"
	 */
	public String getQueryColumn(UserDBProfile profile) {
		
		Map<String, String> columnMap = profile.getAttributesColumnNameMap();
		StringBuffer sColumnBuffer = new StringBuffer();
		
		for (String value : columnMap.values()) {
		    
			sColumnBuffer.append("[").append(value).append("], ");
		}
		
		//To remove the last ","
		if (sColumnBuffer.length()>0) {
			sColumnBuffer.setLength(sColumnBuffer.length() - 2);
		}
		
		return sColumnBuffer.toString();
	}

	/**
	 * Retrieve profile from the provider
	 * @return UserDBProfile object
	 */
	public UserDBProfile getProfile() {	
		
		if(isSingleProfile) {
			return singleProfile;
		}
		else {
			//TODO return matching profile
			return null;
		}
		
	}
	
	/**
	 * Initialize of the Database connection pool
	 * @param profile UserDBProfile
	 */
	public void initDBConnetionPool(UserDBProfile profile) {
		
		ds = new HikariCPDataSource(profile);
		
		String sQueryColumn = getQueryColumn(profile);
		
		mainSQLQuery = mainSQLQuery.replaceAll("##", sQueryColumn);
		
		LOG.debug("Main SQL Query is " + mainSQLQuery);
		
		singleUserSQLQuery = singleUserSQLQuery.replaceAll("##", sQueryColumn);
	}
	
	/**
	 * Retrieve connection from DB connection pool
	 * @return A DB connection from the pool, null if the pool is not initialized.
	 * @throws SQLException Database Exception
	 */
	public synchronized Connection getConnectionFromPool() throws SQLException {
		
		if (ds!=null)
			return ds.getConnection();
		
		LOG.error("Datasource is null and not initliazed");
		
		return null;
	}
		
	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#getUserObject(java.lang.String, java.lang.String)
	 */
	@Override
	public UserObject getUserObject(String id, String attributeToSearch) throws SQLException, NamingException {

		UserObject object = null;

		if (idToObjectTypeMap.get(id) == null
				|| (idToObjectTypeMap.get(id) != null && idToObjectTypeMap.get(id).equals(USER_TYPE))) {

			if (isSingleProfile) {
				
				object = queryForUser(singleProfile, id);

			} else {

				List<String> profilesToLook = userAttributeToProfileMap.get(attributeToSearch.toLowerCase());
				if (profilesToLook == null) {
					LOG.error(String.format("Attribute [%s] isn't provided by any domain", attributeToSearch));
					return null;
				}

				for (String profileName : profilesToLook) {
					UserDBProfile dbProfile = profiles.get(profileName);

					LOG.info(String.format("Attribute [%s] should be found in domain [%s]. Attemp to query...",
							attributeToSearch, dbProfile.getName()));

					object = queryForUser(dbProfile, id);

					if (object != null) {
						break;
					}
				}
			}
		} else {
			LOG.error(String.format("Type cannot be found for ID [%s]", id));
		}

		if (object == null) {
			LOG.error(String.format("Object [%s] cannot be queried from DB", id));
		}

		return object;
	}

	/**
	 * Query for a user with user ID and store in into the cache store
	 * @param dbProfile DBProfile
	 * @param resId User ID
	 * @return UserObject which matched the given user ID
	 * @throws NamingException
	 * @throws SQLException
	 */
	private UserObject queryForUser(UserDBProfile dbProfile, String userId) throws NamingException, SQLException {

		UserObject user = null;
		
		long startTime = System.currentTimeMillis();

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(singleUserSQLQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, userId);
			pst.setString(2, userId);
			try (ResultSet rs = pst.executeQuery()) {
				while (rs.next()) {

					user = produceUser(rs, dbProfile);

					// write user to cache
					UserCacheEngine.getInstance().writeObjectToUserCache(user);

					// update identifier map
					for (String key : dbProfile.getKeyAttributes()) {
						if (user.getAttribute(key.toLowerCase()) != null
								&& user.getAttribute(key.toLowerCase()).getValue() != null) {
							UserCacheEngine.getInstance().addIdentifier(
									(String) user.getAttribute(key.toLowerCase()).getValue(), user.getId());
							idToObjectTypeMap.put((String) user.getAttribute(key.toLowerCase()).getValue(), USER_TYPE);
						}
					}
				}
			}

		}

		long endTime = System.currentTimeMillis();

		LOG.info(String.format("Query for user [%s] took %dms", userId, (endTime - startTime)));

		return user;
	}
	

	
	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#refreshCache()
	 */
	@Override
	public synchronized void refreshCache() {
		
		if (!commonProp.getProperty("refresh","false").equalsIgnoreCase("false")) {

			isRefreshing = true;

			long startTime = System.currentTimeMillis();
			int count = 0;

			while (true) {
				try {
					if (isSingleProfile) {
						refreshProfile(singleProfile);
					} else {
						for (UserDBProfile dbProfile : profiles.values()) {
							refreshProfile(dbProfile);
						}
					}
					break;

				} catch (Exception e) {

					LOG.error("Cache refresh encountered an exception.", e);

					if (count++ == numberOfRetries) {
						LOG.error(String.format("Attempted [%d] retries without success.", numberOfRetries));
						break;
					} else {
						LOG.debug(String.format("Retrying refreshing cache in [%d] seconds..", intervalBetweenRetries));
						try {
							Thread.sleep(intervalBetweenRetries * 1000);
						} catch (InterruptedException ie) {
							// IGNORE
						}
					}
				}
			}

			long endTime = System.currentTimeMillis();

			isRefreshing = false;

			LOG.info("Cache refresh completed");
			LOG.info("Time Taken: " + Long.toString((endTime - startTime)) + "ms");

		}
		else{
			LOG.info("Skip reload cache since the refresh is false");
		}
	}
	
	/**
	 * Reload the cache store which contain User 
	 * @param dbProfile UserDBProfile
	 * @throws NamingException
	 */
	private void refreshProfile(UserDBProfile dbProfile) throws NamingException {
		LOG.info(String.format("Started refreshing domain [%s]", dbProfile.getName()));

		if (!dbProfile.getIsValid()) {
			LOG.error(String.format("Profile [%s] is invalid. Skip refreshing.", dbProfile.getName()));
			return;
		}

		try (Connection conn = getConnectionFromPool()) {
			refreshUser(dbProfile, conn);
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e);
		}
	}
	
	/**
	 * Reload the user object in cache store
	 * @param dbProfile UserDBProfile
	 * @param conn DB Connection
	 * @throws NamingException
	 * @throws SQLException
	 */
	private void refreshUser(UserDBProfile dbProfile, Connection conn) throws NamingException, SQLException {

		UserObject user = null;
		
		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(mainSQLQuery.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = pst.executeQuery();) {
			while (rs.next()) {

				
				user = produceUser(rs, dbProfile);
				
				// write user to cache
				UserCacheEngine.getInstance().writeObjectToUserCache(user);
				
				// update identifier map
				for (String key : dbProfile.getKeyAttributes()) {
					if (user.getAttribute(key.toLowerCase()) != null
							&& user.getAttribute(key.toLowerCase()).getValue() != null) {
						UserCacheEngine.getInstance().addIdentifier(
								(String) user.getAttribute(key.toLowerCase()).getValue(), user.getId());
						idToObjectTypeMap.put((String) user.getAttribute(key.toLowerCase()).getValue(), USER_TYPE);
					}
				}
			}
		}
	}

	
	/**
	 * Produce a UserObject from multiple table
	 * @param rs Resultset contain the main entry
	 * @param profile  UserDBProfile for the profile
	 * @return A UserObject 
	 * @throws NamingException
	 * @throws SQLException
	 */
	private UserObject produceUser(ResultSet rs, UserDBProfile profile) throws NamingException, SQLException {

		UserObject user = null;

		String[] ids = new String[profile.getKeyAttributes().size()];

		for (int i = 0; i < profile.getKeyAttributes().size(); i++) {

			String keyAttributeName = profile.getKeyAttributes().get(i);

			String temp = rs.getString(profile.getKeyAtttributeDBColumnName(profile.getKeyAttributes().get(i)));
			if (temp == null) {
				ids[i] = "UNDEFINED";
			} else {
					ids[i] = temp;

					if (!profile.isKeyCaseSensitive(keyAttributeName)) {
						ids[i] = ids[i].toLowerCase();
					}
			}
		}

		String userId = Util.makeCombinedID(profile.getName(), ids);

		user = new UserObject(profile.getName(), userId, USER_TYPE);

		Set<String> sValues = new TreeSet<>();

		// process attributes to pull
		for (String attributeName: profile.getAttributesToPull()) {

			String temp = rs.getString(profile.getAttributeDBColumnName(attributeName));

			if (!profile.isMultiAttribute(attributeName)) {
				user.addAttribute(attributeName.toLowerCase(),(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
			} else {

				if (temp == null && rs.isLast()) {
					user.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
				} else {
					
					int iCurrentRow = rs.getRow();

					sValues.clear();
					
					temp = rs.getString(profile.getAttributeDBColumnName(attributeName));
					
					if (temp != null && temp.length() > 0) {
						sValues.add(temp.trim());
					}

					while (rs.next()) {
						temp = rs.getString(profile.getAttributeDBColumnName(attributeName));
						if (temp != null && temp.length() > 0) {
							sValues.add(temp.trim());
						}
					}
					
					rs.absolute(iCurrentRow);

					if (sValues.size() > 0) {
						user.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.create(sValues)));
					} else {
						user.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
					}
				}
			}

		}
		
		//Append attribute from another table
		produceLinkAttribute(profile, rs.getString("ID"),user);

		// process key attributes
		for (int i = 0; i < profile.getKeyAttributes().size(); i++) {
			String attributeName = profile.getKeyAttributes().get(i);

			String temp = rs.getString(profile.getKeyAtttributeDBColumnName(attributeName));
			
			if (temp != null) {
				
				user.addAttribute(attributeName.toLowerCase(),
						EvalValue.build((profile.isKeyCaseSensitive(attributeName)) ? temp.toString()
								: temp.toString().toLowerCase()));
			}
		}		
		return user;
	}
	
	/**
	 * Query the attribute value from secondary table
	 * @param profile UserDBProfile profile
	 * @param id User ID
	 * @param user UserObject for storing attribute value
	 * @throws SQLException
	 */
	private void produceLinkAttribute(UserDBProfile profile, String id, UserObject user) throws SQLException {


		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(SQL_LINK_QUERY, ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);) {
				pst.setString(1, id);
			try (ResultSet rs = pst.executeQuery()) {
				if (rs.next()) {

					Set<String> sValues = new TreeSet<>();

					// process attributes to pull
					for (String attributeName : profile.getLinkAttributesToPull()) {

						String temp = rs.getString(profile.getLinkAttributeDBColumnName(attributeName));

						if (!profile.isLinkMultiAttribute(attributeName)) {
							user.addAttribute(attributeName.toLowerCase(),
									(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
						} else {

							if (temp == null && rs.isLast()) {
								user.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
							} else {

								int iCurrentRow = rs.getRow();

								sValues.clear();

								temp = rs.getString(profile.getLinkAttributeDBColumnName(attributeName));

								if (temp != null && temp.length() > 0) {
									sValues.add(temp.trim());
								}

								while (rs.next()) {
									temp = rs.getString(profile.getLinkAttributeDBColumnName(attributeName));
									if (temp != null && temp.length() > 0) {
										sValues.add(temp.trim());
									}
								}

								rs.absolute(iCurrentRow);

								if (sValues.size() > 0) {
									user.addAttribute(attributeName.toLowerCase(),
											EvalValue.build(Multivalue.create(sValues)));
								} else {
									user.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
								}
							}
						}

					}

				}
			}

		}

	}
	

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#loadProfiles(java.util.Properties, java.lang.String)
	 */
	@Override
	public void loadProfiles(Properties props, String propsFilePath) {
		
		profiles = new HashMap<String, UserDBProfile>();

		String sProfileNames = props.getProperty("profile_names");

		if (sProfileNames != null) {
			String[] profileNames = sProfileNames.split(",");

			for (String name : profileNames) {

				name = name.trim();

				LOG.info(String.format("Loading profile of domain [%s]", name));
				UserDBProfile profile = new UserDBProfile(name);
				try {
					profile.parseProfile(props, propsFilePath);
					profiles.put(profile.getName(), profile);

					for (String attr : profile.getAttributesToPull()) {
						if (userAttributeToProfileMap.containsKey(attr.toLowerCase())) {
							userAttributeToProfileMap.get(attr.toLowerCase()).add(profile.getName());
						} else {
							List<String> newIndex = new ArrayList<String>();
							newIndex.add(profile.getName());
							userAttributeToProfileMap.put(attr.toLowerCase(), newIndex);
						}
					}


				} catch (InvalidProfileException ipe) {
					LOG.error(String.format("Invalid profile for domain [%s]", name), ipe);
				}

			}
		} else {
			LOG.warn("Profile names are undefined");
		}

	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#loadSingleProfile(java.util.Properties, java.lang.String)
	 */
	@Override
	public void loadSingleProfile(Properties props, String propsFilePath) {
		String name = "DB_1";

		LOG.info(String.format("Loading profile of domain [%s]", name));
		UserDBProfile profile = new UserDBProfile(name);
		try {
			profile.parseProfile(props, propsFilePath);
			singleProfile = profile;
		} catch (InvalidProfileException ipe) {
			LOG.error(String.format("Invalid profile for domain [%s]", name), ipe);
		}

	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#setIsSingleProfile(java.lang.Boolean)
	 */
	@Override
	public void setIsSingleProfile(Boolean isSingleProfile) {
		this.isSingleProfile = isSingleProfile;
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#isRefreshing()
	 */
	@Override
	public Boolean isRefreshing() {
		return isRefreshing;
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#setCommonProperties(java.util.Properties)
	 */
	@Override
	public void setCommonProperties(Properties props) {
		commonProp = props;
		
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#getItemObject(java.lang.String, java.lang.String)
	 */
	@Override
	public ResourceObject getItemObject(String id, String attributeToSearch) throws Exception {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#getProgramObject(java.lang.String, java.lang.String)
	 */
	@Override
	public ResourceObject getProgramObject(String id, String attributeToSearch) throws Exception {
		return null;
	}

}
