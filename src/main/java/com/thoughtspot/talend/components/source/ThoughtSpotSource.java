package com.thoughtspot.talend.components.source;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.load_utility.TSLoadUtilityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
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
            records = tsLoadUtility.retrieve(this.configuration.getDataset().getDatastore().getDatabase(),
                    this.configuration.getDataset().getTable());
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
        if (records.isEmpty()) {
            LOG.info("TS:: Records Empty");
            return null;
        }
        else {
            LOG.info("TS:: Processing Records for Sample");
            Iterator<String> itr = records.iterator();
            Record.Builder record = builderFactory.newRecordBuilder();
            String[] keys = schema.keySet().toArray(new String[0]);
            String line = null;
            while (itr.hasNext()) {
                line = itr.next();
                String[] fields = line.split("\\|");
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
                    // TODO: Need to add for DateTime and Timestamp

                }
                break;
            }

            records.remove(line);
            return record.build();
        }
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
    }
}
