package com.thoughtspot.talend.components.output;

import java.io.Serializable;

import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" })
})
@Documentation("TODO fill the documentation for this configuration")
public class ThoughtSpotOutputConfiguration implements Serializable {

    @Service
    private RecordBuilderFactory builderFactory;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ThoughtSpotDataset dataset;

    public ThoughtSpotDataset getDataset() {
        return dataset;
    }

    public ThoughtSpotOutputConfiguration setDataset(ThoughtSpotDataset dataset) {
        this.dataset = dataset;
        return this;
    }

    @Producer
    public Record next() {
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the builderFactory to create a new Record.

        Record.Builder record = builderFactory.newRecordBuilder();
        record.withString("Response", "test");
        return record.build();
    }
}