package com.thoughtspot.talend.components;

import java.lang.reflect.Field;
import java.util.Properties;

import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;
import com.thoughtspot.talend.components.datastore.ThoughtSpotDataStore;
import com.thoughtspot.talend.components.output.ThoughtSpotOutput;
import com.thoughtspot.talend.components.output.ThoughtSpotOutputConfiguration;
import com.thoughtspot.talend.components.service.DatastoreValidationService;
import com.thoughtspot.talend.components.service.ThoughtspotComponentService;
import com.thoughtspot.talend.components.source.ThoughtSpotMapperConfiguration;
import com.thoughtspot.talend.components.source.ThoughtSpotSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {}) // EnvironmentConfiguration is necessary for each
                                                                             // @Environment

@WithMavenServers
@WithComponents("com.thoughtspot.talend.components")
@TestMethodOrder(OrderAnnotation.class)
public class ThoughtSpotTests extends ThoughtSpotBaseTest {
    
    @Service
    private DatastoreValidationService service;

    @Service
    private ThoughtspotComponentService componentService;

    private ThoughtSpotDataStore datastore;

    private Properties props;

    @BeforeEach
    public void setup()
    {
        props = tsProps();
        datastore = buildDataStore(props);
    }


    @EnvironmentalTest
    @Order(1)
    public void validateConnectionTest() {
        final HealthCheckStatus status = service.healthCheck(datastore);
        Assertions.assertNotNull(status);
        Assertions.assertEquals(HealthCheckStatus.Status.OK,status.getStatus());
    }

    @EnvironmentalTest
    @Order(2)
    public void readerTest() {
        final ThoughtSpotDataset dataset = new ThoughtSpotDataset();
        dataset.setDatastore(datastore);
        dataset.setDatabase(props.getProperty("ts.database"));
        dataset.setTable(props.getProperty("ts.src.table"));
        dataset.setSchema(props.getProperty("ts.schema"));
        dataset.setCreateTable(false);
        dataset.setCreateDatabase(false);
        dataset.setCreateSchema(false);
        final ThoughtSpotMapperConfiguration configuration = new ThoughtSpotMapperConfiguration();
        configuration.setDataset(dataset);
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl(null);
        ThoughtSpotSource source = new ThoughtSpotSource(configuration, componentService, factory);
        source.init();
        Record record = source.next();
        Assertions.assertNotNull(record);
        source.release();
    }

    @EnvironmentalTest
    @Order(3)
    public void writerTest() throws IllegalAccessException, NoSuchFieldException {
        final ThoughtSpotDataset sourceDataset = new ThoughtSpotDataset();
        sourceDataset.setDatastore(datastore);
        sourceDataset.setDatabase(props.getProperty("ts.database"));
        sourceDataset.setTable(props.getProperty("ts.src.table"));
        sourceDataset.setSchema(props.getProperty("ts.schema"));
        sourceDataset.setCreateTable(false);
        sourceDataset.setCreateDatabase(false);
        sourceDataset.setCreateSchema(false);

        final ThoughtSpotDataset targetDataset = new ThoughtSpotDataset();
        targetDataset.setDatastore(datastore);
        targetDataset.setDatabase(props.getProperty("ts.database"));
        targetDataset.setTable(props.getProperty("ts.tgt.table"));
        targetDataset.setSchema(props.getProperty("ts.schema"));
        targetDataset.setCreateTable(true);
        targetDataset.setCreateDatabase(false);
        targetDataset.setCreateSchema(false);

        final ThoughtSpotMapperConfiguration configuration = new ThoughtSpotMapperConfiguration();
        configuration.setDataset(sourceDataset);
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl(null);
        
        ThoughtSpotOutputConfiguration outputConfiguration = new ThoughtSpotOutputConfiguration();
        outputConfiguration.setTruncate(false);
        outputConfiguration.setDataset(targetDataset);
        outputConfiguration.setMaxIgnoredRows(10);
        outputConfiguration.setBadRecordsFile("");
        outputConfiguration.setDateFormat("%Y-%m-%d");
        outputConfiguration.setDateTimeFormat("%Y-%m-%d %H:%M:%S");
        outputConfiguration.setTimeFormat("%H:%M:%S");
        outputConfiguration.setVerbosity(0);
        outputConfiguration.skipSecondFraction(false);
        outputConfiguration.setDateConvertedToEpoch(false);
        outputConfiguration.setBooleanRepresentation("T_F");
        
        final Field recordBuilderField = ThoughtSpotOutputConfiguration.class.getDeclaredField("builderFactory");
        final Field _MAX_TSLOADER_THREADS = ThoughtSpotOutput.class.getDeclaredField("_MAX_TSLOADER_THREADS");
        recordBuilderField.setAccessible(true);
        recordBuilderField.set(outputConfiguration, factory);
        

        SuggestionValues before = DatastoreValidationService.listTables(datastore, props.getProperty("ts.database"), props.getProperty("ts.schema"));

        ThoughtSpotOutput output = new ThoughtSpotOutput(outputConfiguration, componentService);
        _MAX_TSLOADER_THREADS.setAccessible(true);
        _MAX_TSLOADER_THREADS.set(output, 1);
        ThoughtSpotSource source = new ThoughtSpotSource(configuration, componentService, factory);
        source.init();
        output.init();

        Record record;
        while ((record = source.next()) != null)
        {
            output.onNext(record);
        }
        Assertions.assertEquals(null, record);
        output.afterGroup();
        source.release();
        output.release();
        SuggestionValues after = DatastoreValidationService.listTables(datastore, props.getProperty("ts.database"), props.getProperty("ts.schema"));
        
        //If false then table created.
        Assertions.assertFalse(after.getItems().size() == before.getItems().size());
        
    }
}