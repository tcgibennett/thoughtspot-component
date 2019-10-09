package com.thoughtspot.talend.components.source;

import java.io.Serializable;

import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" })
})
@Documentation("TODO fill the documentation for this configuration")
public class ThoughtSpotMapperConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ThoughtSpotDataset dataset;

    public ThoughtSpotDataset getDataset() {
        return dataset;
    }

    public ThoughtSpotMapperConfiguration setDataset(ThoughtSpotDataset dataset) {
        this.dataset = dataset;
        return this;
    }
}