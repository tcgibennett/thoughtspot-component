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
        @GridLayout.Row({ "table" }),
        @GridLayout.Row({"createTable"})

})
@Documentation("TODO fill the documentation for this configuration")
public class ThoughtSpotDataset implements Serializable {
    private static final transient Logger LOG = LoggerFactory.getLogger(ThoughtSpotDataset.class);

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ThoughtSpotDataStore datastore;

    @Option
    @Suggestable(value = "showTables", parameters = {"datastore"})
    @Documentation("TODO fill the documentation for this parameter")
    private String table;
    


/*
    @Option
    @Structure(type = Structure.Type.OUT, discoverSchema = "discover",value="ThoughtSpot")
    @Documentation("TODO place to capture table definition")
    private List<String> fields = new ArrayList<>();
*/
    @Option
    @Documentation("Create table if does not exist")
    private boolean createTable = false;

    public ThoughtSpotDataStore getDatastore() {
        return datastore;
    }

    public ThoughtSpotDataset setDatastore(ThoughtSpotDataStore datastore) {
        this.datastore = datastore;
        return this;
    }

    public String getTable() {
        return table;
    }

    public ThoughtSpotDataset setTable(String table) {
        this.table = table;
        return this;
    }
    

/*
    public List<String> getFields() {
    	return fields;
    }
    
    public ThoughtSpotDataset setFields(List<String> fields) {
    	this.fields = fields;
    	return this;
    }
*/
    public boolean getCreateTable() { return createTable; }

    public ThoughtSpotDataset setCreateTable(boolean createTable)
    {
        this.createTable = createTable;
        return this;
    }

    public LinkedHashMap<String, String> getTableColumns() {
    	TSLoadUtility utility = TSLoadUtility.getInstance(this.getDatastore().getHost(), this.getDatastore().getPort(), this.getDatastore().getUsername(), this.getDatastore().getPassword());
    	LinkedHashMap<String, String> rs = null;
    	try {
    		utility.connect();
    		String[] parts = this.getTable().split("\\.");
    		rs = utility.getTableColumns(this.getDatastore().getDatabase(), parts[0], parts[1]);
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