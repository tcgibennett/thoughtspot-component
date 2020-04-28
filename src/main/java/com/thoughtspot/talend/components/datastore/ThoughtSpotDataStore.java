package com.thoughtspot.talend.components.datastore;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.completion.SuggestionValues;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.load_utility.TSLoadUtilityException;

@DataStore("ThoughtSpotDataStore")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
        @GridLayout.Row({ "host" }),
        @GridLayout.Row({ "port" }),
        @GridLayout.Row({ "username" }),
        @GridLayout.Row({ "password" })
})

@Checkable("DatastoreConnection")
@Documentation("TODO fill the documentation for this configuration")
public class ThoughtSpotDataStore implements Serializable {
	
	
    @Option
    @Documentation("The host or ip of ThoughtSpot server")
    private String host;
    
    @Option
    @Documentation("The ssh port of ThoughtSpot server")
    private int port = 22;

    @Option
    @Documentation("The ssh username of ThoughtSpot server")
    private String username;

    @Credential
    @Option
    @Documentation("The ssh password of the ThoughtSpot server")
    private String password;

    

    public String getHost() {
        return host;
    }

    public ThoughtSpotDataStore setHost(String host) {
        this.host = host;
        return this;
    }
    
   
    
    public int getPort() {
        return port;
    }

    public ThoughtSpotDataStore setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ThoughtSpotDataStore setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ThoughtSpotDataStore setPassword(String password) {
        this.password = password;
        return this;
    }


    public boolean getConnection()
    {
    	TSLoadUtility conn = null;
    	boolean flag = false;
    	conn = TSLoadUtility.getInstance(this.getHost(),this.port, this.getUsername(), this.getPassword());
    	try {
    		conn.connect();
    		flag = true;
    	} catch(TSLoadUtilityException e)
    	{
    		e.printStackTrace();
    	} 
    	if (flag) conn.disconnect();
    	return flag;
    }
    
    public List<SuggestionValues.Item> getTables(String database, String schema) {
    	List<SuggestionValues.Item> tables = new ArrayList<SuggestionValues.Item>();
    	try {
    		TSLoadUtility conn = TSLoadUtility.getInstance(this.getHost(),this.port, this.getUsername(), this.getPassword());
    		conn.connect();
    		ArrayList<String> rs = conn.getTables(database, schema);
    		for (String table : rs)
    		{
    		    tables.add(new SuggestionValues.Item(table,table));
    		}
    		conn.disconnect();	
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	
    	return tables;
    }

    public List<SuggestionValues.Item> getDatabases() {
    	List<SuggestionValues.Item> databases = new ArrayList<SuggestionValues.Item>();
    	try {
    		TSLoadUtility conn = TSLoadUtility.getInstance(this.getHost(),this.port, this.getUsername(), this.getPassword());
    		conn.connect();
    		ArrayList<String> rs = conn.getDatabases();
    		for (String database : rs)
    		{
    		    databases.add(new SuggestionValues.Item(database,database));
    		}
    		conn.disconnect();	
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	
    	return databases;
    }


    public List<SuggestionValues.Item> getSchemas(String database) {
    	List<SuggestionValues.Item> schemas = new ArrayList<SuggestionValues.Item>();
    	try {
    		TSLoadUtility conn = TSLoadUtility.getInstance(this.getHost(),this.port, this.getUsername(), this.getPassword());
    		conn.connect();
    		ArrayList<String> rs = conn.getSchemas(database);
    		for (String schema : rs)
    		{
    		    schemas.add(new SuggestionValues.Item(schema,schema));
    		}
    		conn.disconnect();	
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	
    	return schemas;
    }
}