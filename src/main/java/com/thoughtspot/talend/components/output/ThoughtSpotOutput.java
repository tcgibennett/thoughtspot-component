package com.thoughtspot.talend.components.output;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.thoughtspot.load_utility.TSReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
@Icon(value = Icon.IconType.CUSTOM, custom = "ThoughtSpot") // you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding icons/filename.svg in resources
@Processor(name = "Output")
@Documentation("TODO fill the documentation for this processor")
public class ThoughtSpotOutput implements Serializable {
    private final ThoughtSpotOutputConfiguration configuration;
    private final ThoughtspotComponentService service;
	private static final transient Logger LOG = LoggerFactory.getLogger(ThoughtSpotOutput.class);
    private List<Record> records = null;
    private TSLoadUtility tsloader = null;
    private ArrayList<String> tables = null;
    private boolean createTable = false;
	private LinkedHashMap<String, String> ts_schema = null;
	private TSReader tsReader = null;
	private List<ExecutorService> threads = new ArrayList<ExecutorService>();
	private List<TSLoadUtility> loaders = new ArrayList<TSLoadUtility>();
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

		LOG.info("ThoughtSpot::Creating New Instance of TSLoadUtility");
    	try {
			tsloader.connect();
			LOG.info("ThoughtSpot Connection Successful");

			this.createTable = this.configuration.getDataset().getCreateTable();
			LOG.info("ThoughtSpot::CreateTableFlag->" + this.createTable);


			tsloader.setTSLoadProperties(this.configuration.getDataset().getDatastore().getDatabase(), this.configuration.getDataset().getTable().split("\\.")[1], ",");

			LOG.info("ThoughtSpot::Set Load Properties");
    	} catch(TSLoadUtilityException e)
    	{
    		LOG.error("TSLoadUtilityException " + e.getMessage());
    	}
		tsReader = TSReader.newInstance();
		for (int x = 0; x < this.configuration.getDataset().getDatastore().getLoaderProcesses(); x++)
		{
			threads.add(Executors.newSingleThreadExecutor());
			TSLoadUtility tsLoadUtility = TSLoadUtility.getInstance(this.configuration.getDataset().getDatastore().getHost(),this.configuration.getDataset().getDatastore().getPort(),this.configuration.getDataset().getDatastore().getUsername(),this.configuration.getDataset().getDatastore().getPassword());
			try {
				tsLoadUtility.connect();
				tsLoadUtility.setTSLoadProperties(this.configuration.getDataset().getDatastore().getDatabase(), this.configuration.getDataset().getTable().split("\\.")[1], ",");
			} catch (TSLoadUtilityException e) {
				LOG.error(e.getMessage());
			}

			loaders.add(tsLoadUtility);
		}


		for (ExecutorService executorService : threads)
		{
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					try {
						TSLoadUtility loadUtility = loaders.remove(0);
						loadUtility.loadData(tsReader);
						loadUtility.disconnect();
					} catch (TSLoadUtilityException e) {
						LOG.error(e.getMessage());
					}
					//Thread.yield();
				}
			});

		}
		//private ExecutorService executorService = Executors.newSingleThreadExecutor();

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

		try {
			if (this.createTable)
			{

					tables = this.tsloader.getTables(this.configuration.getDataset().getDatastore().getDatabase());
					boolean tableExists = false;
					for (String table : tables) {
						if (table.equals(this.configuration.getDataset().getTable()))
							tableExists = true;
					}
					if (!tableExists)
						this.createTableDDL(defaultInput.getSchema().getEntries());


					this.createTable = false;


			}
			if (ts_schema == null) {
				ts_schema = tsloader.getTableColumns(this.configuration.getDataset().getDatastore().getDatabase(),
						this.configuration.getDataset().getTable().split("\\.")[0],
						this.configuration.getDataset().getTable().split("\\.")[1]);
				LOG.info("TS_SCHEMA::" + ts_schema.size());
			}
		} catch(TSLoadUtilityException e)
		{
			LOG.error("TSLoadUtilityException " + e.getMessage());
		}
		//this.insertRecord(defaultInput);
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
		this.insertRecords(records);
		this.records.clear();
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        // Note: if you don't need it you can delete it
    	if (this.records.size() > 0)
    		this.insertRecords(records);

		this.tsReader.setIsCompleted(true);
		while(!this.tsReader.done())
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
		for (ExecutorService executorService : threads)
			executorService.shutdown();
    	this.tsloader.disconnect();
    }

    private void createTableDDL(List<Schema.Entry> entries)
	{
		LOG.info("ThoughtSpot::Create Table");
		LOG.info("ThoughtSpot::Entries " + entries.size());
		LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
			for (Schema.Entry entry : entries) {
					if (entry.getType() == Schema.Type.STRING)
						attributes.put(entry.getName(), "varchar(255)");
					else if (entry.getType() == Schema.Type.DOUBLE)
						attributes.put(entry.getName(), "double");
					else if (entry.getType() == Schema.Type.FLOAT)
						attributes.put(entry.getName(), "float");
					else if (entry.getType() == Schema.Type.INT)
						attributes.put(entry.getName(), "int");
					else if (entry.getType() == Schema.Type.LONG)
						attributes.put(entry.getName(), "bigint");
					else if (entry.getType() == Schema.Type.BOOLEAN)
						attributes.put(entry.getName(), "bool");
					else if (entry.getType() == Schema.Type.DATETIME)
						attributes.put(entry.getName(), "datetime");
					else
						LOG.warn("ThoughtSpot Unknown DataType " + entry.getType().name());
			}

		try {
			LOG.info("ThoughtSpot::Attempting to create Table " + this.configuration.getDataset().getTable());
			this.tsloader.createTable(attributes, this.configuration.getDataset().getTable(), this.configuration.getDataset().getDatastore().getDatabase());
			LOG.info("ThoughtSpot::" + this.configuration.getDataset().getTable() + " created successfully");
		} catch(Exception e) {
			LOG.info("ThoughtSpot Create Table Failed::" + e.getMessage());
		}
	}

    private void insertRecords(List<Record> records) {
    	StringBuilder rec = null;
    	Table table = null;
    	ArrayList<String> recs = new ArrayList<String>();
		LOG.info("TS:: Records To Load " + records.size());
    	for (Record record : records) {
    		rec = new StringBuilder();

    		Schema schema = record.getSchema();
			table = new Table(schema.getEntries().size());
				int i = 0;
				Set<String> keys = ts_schema.keySet();
				for (String key : keys) {

					for (Schema.Entry entry : schema.getEntries()) {
						if (entry.getName().equalsIgnoreCase(key)) {
							if (ts_schema.get(key).equalsIgnoreCase("varchar")) {
								table.add(record.getString(entry.getName()), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else if (ts_schema.get(key).equalsIgnoreCase("double")) {
								table.add(String.valueOf(record.getInt(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else if (ts_schema.get(key).equalsIgnoreCase("int32")) {
								table.add(String.valueOf(record.getInt(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else if (ts_schema.get(key).equalsIgnoreCase("float")) {
								table.add(String.valueOf(record.getFloat(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else if (ts_schema.get(key).equalsIgnoreCase("bigint")) {
								table.add(String.valueOf(record.getLong(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else if (ts_schema.get(key).equalsIgnoreCase("bool")) {
								table.add(String.valueOf(record.getBoolean(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else if (ts_schema.get(key).equalsIgnoreCase("date") ||
									ts_schema.get(key).equalsIgnoreCase("datetime") ||
									ts_schema.get(key).equalsIgnoreCase("time")) {
								table.add(String.valueOf(record.getDateTime(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
							else {
								table.add(String.valueOf(record.getDateTime(entry.getName())), i++);
								//System.out.println(key +"--"+ts_schema.get(key)+"--"+entry.getType());
								break;
							}
						}
					}
				}
			//System.out.println(table.toString());
    		//recs.add(table.toString());
				this.tsReader.add(table.toString());
    		
    	}

    	/*
    	try {
			this.tsloader.loadData(recs);
		} catch (TSLoadUtilityException e) {
			// TODO Auto-generated catch block
			LOG.error("TSLoadUtilityException::" + e.getMessage());
		}
    	*/
    	
    }


}