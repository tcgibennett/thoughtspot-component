package com.thoughtspot.talend.components.service;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.load_utility.TSLoadUtilityException;
import com.thoughtspot.talend.components.datastore.ThoughtSpotDataStore;

import static java.util.Collections.singletonList;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;


@Service
public class DatastoreValidationService {
	
	
	
	
	@HealthCheck(value = "DatastoreConnection")
	public HealthCheckStatus healthCheck(@Option ThoughtSpotDataStore conn)
	{
			if (getConnection(conn))
				return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Success");
			else
				return new HealthCheckStatus(HealthCheckStatus.Status.KO, "Failed");
	}
	
	public boolean getConnection(ThoughtSpotDataStore dataStore)
    {
    	TSLoadUtility conn = null;
    	boolean flag = false;
    	conn = TSLoadUtility.getInstance(dataStore.getHost(),dataStore.getPort(), dataStore.getUsername(), dataStore.getPassword());
    	try {
    		conn.connect();
    		flag = true;
    	} catch(TSLoadUtilityException e)
    	{
    		throw new IllegalArgumentException(e.getMessage());
    	} 
    	if (flag) conn.disconnect();
    	return flag;
    }
	
	@Suggestions("showTables")
	public static SuggestionValues listTables(@Option ThoughtSpotDataStore datastore,
	@Option String database, @Option String schema)
	{
		
		SuggestionValues values = new SuggestionValues();
		values.setItems(getTables(datastore, database, schema));
		
		return values;
	}

	@Suggestions("showSchemas")
	public static SuggestionValues listSchemas(@Option ThoughtSpotDataStore datastore,
	@Option String database)
	{
		
		SuggestionValues values = new SuggestionValues();
		values.setItems(getSchemas(datastore, database));
		
		return values;
	}

	@Suggestions("showDatabases")
	public static SuggestionValues listDatabases(@Option ThoughtSpotDataStore datastore)
	{
		
		SuggestionValues values = new SuggestionValues();
		values.setItems(getDatabases(datastore));
		
		return values;
	}

	private static List<SuggestionValues.Item> getDatabases(ThoughtSpotDataStore dataStore) {
    	List<SuggestionValues.Item> databases = new ArrayList<SuggestionValues.Item>();
    	try {
    		TSLoadUtility conn = TSLoadUtility.getInstance(dataStore.getHost(),dataStore.getPort(), dataStore.getUsername(), dataStore.getPassword());
    		conn.connect();
    		ArrayList<String> rs = conn.getDatabases();
    		for (String database : rs)
    		{
    		    databases.add(new SuggestionValues.Item(database,database));
    		}
    		conn.disconnect();	
    	} catch(Exception e) {
    		return databases;
    	}
    	
    	return databases;
	}
	
	private static List<SuggestionValues.Item> getSchemas(ThoughtSpotDataStore dataStore, String database) {
    	List<SuggestionValues.Item> schemas = new ArrayList<SuggestionValues.Item>();
    	try {
    		TSLoadUtility conn = TSLoadUtility.getInstance(dataStore.getHost(),dataStore.getPort(), dataStore.getUsername(), dataStore.getPassword());
    		conn.connect();
    		ArrayList<String> rs = conn.getSchemas(database);
    		for (String schema : rs)
    		{
    		    schemas.add(new SuggestionValues.Item(schema,schema));
    		}
    		conn.disconnect();	
    	} catch(Exception e) {
    		return schemas;
    	}
    	
    	return schemas;
	}

	private static List<SuggestionValues.Item> getTables(ThoughtSpotDataStore dataStore, String database, String schema) {
    	List<SuggestionValues.Item> tables = new ArrayList<SuggestionValues.Item>();
    	try {
    		TSLoadUtility conn = TSLoadUtility.getInstance(dataStore.getHost(),dataStore.getPort(), dataStore.getUsername(), dataStore.getPassword());
    		conn.connect();
    		ArrayList<String> rs = conn.getTables(database, schema);
    		for (String table : rs)
    		{
    		    tables.add(new SuggestionValues.Item(table,table));
    		}
    		conn.disconnect();	
    	} catch(Exception e) {
    		return tables;
    	}
    	
    	return tables;
    }
}
