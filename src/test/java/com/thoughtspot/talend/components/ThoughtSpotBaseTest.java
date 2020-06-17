package com.thoughtspot.talend.components;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;
import com.thoughtspot.talend.components.datastore.ThoughtSpotDataStore;

public class ThoughtSpotBaseTest {
    
    protected Properties tsProps() {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("thoughtspotConfig.properties")) {
            Properties props = new Properties();
            props.load(in);
            return props;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected ThoughtSpotDataStore buildDataStore(Properties props) {
        
        ThoughtSpotDataStore datastore = new ThoughtSpotDataStore();
        datastore.setHost(props.getProperty("ts.host"));
        datastore.setPort(Integer.parseInt(props.getProperty("ts.port")));
        datastore.setUsername(props.getProperty("ts.user"));
        datastore.setPassword(props.getProperty("ts.pwd"));
        return datastore;
    }
}