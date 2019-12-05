package com.thoughtspot.talend.components.source;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.load_utility.TSLoadUtilityException;
import com.thoughtspot.load_utility.TSReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;


import com.thoughtspot.talend.components.service.ThoughtspotComponentService;

@Documentation("TODO fill the documentation for this source")
public class ThoughtSpotSource implements Serializable {
    private final ThoughtSpotMapperConfiguration configuration;
    private final ThoughtspotComponentService service;
    private final RecordBuilderFactory builderFactory;
    private TSLoadUtility tsLoadUtility = null;
    private static final transient Logger LOG = LoggerFactory.getLogger(ThoughtSpotSource.class);
    private ResultSet rs = null;
    private LinkedHashMap<String, String> schema;
    private LinkedHashSet<String> records;
    private TSReader tsReader = null;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public ThoughtSpotSource(@Option("configuration") final ThoughtSpotMapperConfiguration configuration,
                             final ThoughtspotComponentService service,
                             final RecordBuilderFactory builderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
    }

    @PostConstruct
    public void init() {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        tsLoadUtility = TSLoadUtility.getInstance(this.configuration.getDataset().getDatastore().getHost(),
                this.configuration.getDataset().getDatastore().getPort(),this.configuration.getDataset().getDatastore().getUsername(),
                this.configuration.getDataset().getDatastore().getPassword());
        try {
            tsLoadUtility.connect();
            schema = tsLoadUtility.getTableColumns(this.configuration.getDataset().getDatastore().getDatabase(),
                    this.configuration.getDataset().getTable().split("\\.")[0],
                    this.configuration.getDataset().getTable().split("\\.")[1]);
            //tsLoadUtility.retrieve(this.configuration.getDataset().getDatastore().getDatabase(),
                    //this.configuration.getDataset().getTable(), null);
            LOG.info("TS:: pull from ThoughtSpot Table " + this.configuration.getDataset().getTable() +" success!");
        } catch(TSLoadUtilityException e)
        {
            LOG.error(e.getMessage());
        }

    }

    @Producer
    public Record next() {
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the builderFactory to create a new Record.
        if (tsReader == null)
        {
            tsReader = TSReader.newInstance(0);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    tsLoadUtility.retrieve(configuration.getDataset().getDatastore().getDatabase(),
                            configuration.getDataset().getTable(), tsReader);
                    Thread.yield();
                }
            });
        }

        Record.Builder record = builderFactory.newRecordBuilder();
        String[] keys = schema.keySet().toArray(new String[0]);

        String result = null;
        while(true)
        {
            result = tsReader.poll();

            if (result != null)
            {
                String[] fields = result.split("\\|");
                for (int x = 0; x < fields.length; x++) {
                    String field = fields[x];
                    String type = schema.get(keys[x]);
                    //LOG.info("TS:: " + keys[x] + " | " + field + " | " + type);
                    if (type.equalsIgnoreCase("int32"))
                        record.withInt(keys[x], Integer.valueOf(field));
                    else if (type.equalsIgnoreCase("bigint"))
                        record.withLong(keys[x],Long.valueOf(field));
                    else if (type.equalsIgnoreCase("double"))
                        record.withDouble(keys[x], Double.valueOf(field));
                    else if (type.equalsIgnoreCase("float"))
                        record.withFloat(keys[x],Float.valueOf(field));
                    else if (type.equalsIgnoreCase("varchar"))
                        record.withString(keys[x], field);
                    else if (type.equalsIgnoreCase("bool"))
                        record.withBoolean(keys[x], Boolean.valueOf(field));
                    else if (type.equalsIgnoreCase("date")) {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            record.withDateTime(keys[x], dateFormat.parse(field));

                        } catch(ParseException e)
                        {
                            LOG.error("ThoughtSpot:Date:"+e.getMessage());
                        }
                    }
                    else if (type.equalsIgnoreCase("datetime"))
                    {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            record.withDateTime(keys[x], dateFormat.parse(field));
                        } catch(ParseException e)
                        {
                            LOG.error("ThoughtSpot:DateTime:"+e.getMessage());
                        }
                    }
                    else if (type.equalsIgnoreCase("time"))
                    {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
                        try {
                            record.withDateTime(keys[x], dateFormat.parse(field));
                        } catch(ParseException e)
                        {
                            LOG.error("ThoughtSpot:Time:"+e.getMessage());
                        }
                    }

                }
                return record.build();
            } else if (tsReader.getIsCompleted())
            {
                LOG.info("Done Reading Records");
                executorService.shutdown();
                return null;
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
    }
}
