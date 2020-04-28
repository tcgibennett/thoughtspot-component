package com.thoughtspot.talend.components.dataset;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.talend.components.datastore.ThoughtSpotDataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

@DataSet("ThoughtSpotDataset")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
        @GridLayout.Row({ "datastore" }),
        @GridLayout.Row({"database"}),
        @GridLayout.Row({"schema"}),
        @GridLayout.Row({ "table" }),
        @GridLayout.Row({"createDatabase","createSchema","createTable"})

})
@Documentation("TODO fill the documentation for this configuration")
public class ThoughtSpotDataset implements Serializable {
    private static final transient Logger LOG = LoggerFactory.getLogger(ThoughtSpotDataset.class);

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ThoughtSpotDataStore datastore;

    @Option
    @Suggestable(value = "showSchemas", parameters = {"datastore", "database"})
    @Documentation("Displays a list of schemas for given database")
    private String schema = "falcon_default_schema";

    @Option
    @Suggestable(value = "showTables", parameters = {"datastore", "database","schema"})
    @Documentation("Displays a list of tables for selected schema")
    private String table;
    
    @Option
    @Suggestable(value = "showDatabases", parameters = {"datastore"})
    @Documentation("The database in ThoughtSpot server for this user")
    private String database;

    @Option
    @Documentation("Create table if does not exist")
    private boolean createTable = false;

    @Option
    @Documentation("Create Schema if does not exist")
    private boolean createSchema = false;

    @Option
    @Documentation("Create Database if does not exist")
    private boolean createDatabase = false;

    public ThoughtSpotDataStore getDatastore() {
        return datastore;
    }

    public ThoughtSpotDataset setDatastore(ThoughtSpotDataStore datastore) {
        this.datastore = datastore;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public String getDatabase() {
        return database;
    }

    public ThoughtSpotDataset setDatabase(String database) {
        this.database = database;
        return this;
    }

    public ThoughtSpotDataset setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getTable() {
        return table;
    }

    public ThoughtSpotDataset setTable(String table) {
        this.table = table;
        return this;
    }

    public boolean getCreateTable() { return createTable; }

    public ThoughtSpotDataset setCreateTable(boolean createTable)
    {
        this.createTable = createTable;
        return this;
    }

    public boolean getCreateDatabase() { return createDatabase; }

    public ThoughtSpotDataset setCreateDatabase(boolean createDatabase)
    {
        this.createDatabase = createDatabase;
        return this;
    }

    public boolean getCreateSchema() { return createSchema; }

    public ThoughtSpotDataset setCreateSchema(boolean createSchema)
    {
        this.createSchema = createSchema;
        return this;
    }

    public LinkedHashMap<String, String> getTableColumns() {
    	TSLoadUtility utility = TSLoadUtility.getInstance(this.getDatastore().getHost(), this.getDatastore().getPort(), this.getDatastore().getUsername(), this.getDatastore().getPassword());
    	LinkedHashMap<String, String> rs = null;
    	try {
    		utility.connect();
    		rs = utility.getTableColumns(this.getDatabase(), this.getSchema(), this.getTable());
    		utility.disconnect();
    	} catch(Exception e) {
    		LOG.error("TS:: " + e.getMessage());
    	}
    	
    	return rs;
    }
    
    
    
    private String getColumnNames(Schema schema)
    {
    	StringBuilder buff = new StringBuilder();
    	int i = 1;
		for (Schema.Entry entry : schema.getEntries())
		{
			if (i == schema.getEntries().size())
				buff.append(entry.getName());
			else
				buff.append(entry.getName()+",");
			i++;
		}
		
		return buff.toString();
    }
}