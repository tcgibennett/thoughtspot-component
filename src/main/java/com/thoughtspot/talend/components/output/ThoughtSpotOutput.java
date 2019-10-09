package com.thoughtspot.talend.components.output;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.load_utility.TSLoadUtilityException;
import com.thoughtspot.talend.components.service.ThoughtspotComponentService;

@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(Icon.IconType.STAR) // you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding icons/filename.svg in resources
@Processor(name = "Output")
@Documentation("TODO fill the documentation for this processor")
public class ThoughtSpotOutput implements Serializable {
    private final ThoughtSpotOutputConfiguration configuration;
    private final ThoughtspotComponentService service;
    
    private List<Record> records = null;
    private TSLoadUtility tsloader = null;
    private ArrayList<String> tables = null;
    private boolean createTable = false;
    public ThoughtSpotOutput(@Option("configuration") final ThoughtSpotOutputConfiguration configuration,
                          final ThoughtspotComponentService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        // Note: if you don't need it you can delete it
    	records = new ArrayList<Record>();
    	tsloader = TSLoadUtility.getInstance(this.configuration.getDataset().getDatastore().getHost(),this.configuration.getDataset().getDatastore().getPort(),this.configuration.getDataset().getDatastore().getUsername(),this.configuration.getDataset().getDatastore().getPassword());
    	try {
    	tsloader.connect();
    	tables = this.tsloader.getTables(this.configuration.getDataset().getDatastore().getDatabase());
    	this.createTable = this.configuration.getDataset().getCreateTable();

    	tsloader.setTSLoadProperties(this.configuration.getDataset().getDatastore().getDatabase(), this.configuration.getDataset().getTable().split("\\.")[1], ",");
    	} catch(TSLoadUtilityException e)
    	{
    		e.printStackTrace();
    	}
    }

    @BeforeGroup
    public void beforeGroup() {
        // if the environment supports chunking this method is called at the beginning if a chunk
        // it can be used to start a local transaction specific to the backend you use
        // Note: if you don't need it you can delete it
    }

    @ElementListener
    public void onNext(
            @Input final Record defaultInput) {
        // this is the method allowing you to handle the input(s) and emit the output(s)
        // after some custom logic you put here, to send a value to next element you can use an
        // output parameter and call emit(value).

    	this.records.add(defaultInput);
		if (this.createTable)
		{

				boolean tableExists = false;
				for (String table : tables)
				{
					if (table.equals(this.configuration.getDataset().getTable()))
						tableExists = true;
				}
				if (!tableExists)
					this.createTableDDL(defaultInput.getSchema().getEntries());


			this.createTable = false;
		}
    	if (records.size() == this.configuration.getDataset().getBatchSize())
    	{
    		this.insertRecords(records);
    		this.records.clear();
    	}
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        // Note: if you don't need it you can delete it
    	if (this.records.size() > 0)
    		this.insertRecords(records);
    	this.tsloader.disconnect();
    }

    private void createTableDDL(List<Schema.Entry> entries)
	{
		LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
		List<String> fields = this.configuration.getDataset().getFields();
		for (int i = 0; i < fields.size(); i++) {
			for (Schema.Entry entry : entries) {
				if (entry.getName().equals(fields.get(i))) {
					if (entry.getType() == Schema.Type.STRING)
						attributes.put(entry.getName(), "varchar(255)");
					else if (entry.getType() == Schema.Type.DOUBLE)
						attributes.put(entry.getName(), "int");
				}
			}
		}
		try {
			this.tsloader.createTable(attributes, this.configuration.getDataset().getTable(), this.configuration.getDataset().getDatastore().getDatabase());
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

    private void insertRecords(List<Record> records) {
    	StringBuilder rec = null;
    	Table table = null;
    	ArrayList<String> recs = new ArrayList<String>();

    	for (Record record : records) {
    		rec = new StringBuilder();

    		Schema schema = record.getSchema();
			table = new Table(schema.getEntries().size());
    		List<String> fields = this.configuration.getDataset().getFields();
    		for (Schema.Entry entry : schema.getEntries())
    		{
    			for(int i = 0; i < fields.size(); i++) {
					if (entry.getName().equalsIgnoreCase(fields.get(i))) {
						if (entry.getType() == Schema.Type.STRING)
							table.add(record.getString(entry.getName()), i);
						else if (entry.getType() == Schema.Type.DOUBLE)
							table.add(String.valueOf(record.getInt(entry.getName())), i);
					}
				}
    		}	
    		recs.add(table.toString());
    		
    	}
    	try {
			this.tsloader.loadData(recs);
		} catch (TSLoadUtilityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }
}