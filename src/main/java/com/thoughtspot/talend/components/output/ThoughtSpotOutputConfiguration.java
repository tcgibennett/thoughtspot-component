package com.thoughtspot.talend.components.output;

import java.io.Serializable;

import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" }),
        @GridLayout.Row({"truncate","maxIgnoredRows"}),
        @GridLayout.Row({"badRecordsFile","dateFormat"}),
        @GridLayout.Row({"dateTimeFormat", "timeFormat"}),
        @GridLayout.Row({"verbosity", "skipSecondFraction"}),
        @GridLayout.Row({"dateConvertedToEpoch","booleanRepresentation"})
})

@Documentation("TODO fill the documentation for this configuration")
public class ThoughtSpotOutputConfiguration implements Serializable {

    @Service
    private RecordBuilderFactory builderFactory;

    @Option
    @Documentation("The Dataset Object that represents the Table and Schema values")
    private ThoughtSpotDataset dataset;

    @Option
    @Documentation("tsload --empty_target")
    private boolean truncate;

    @Option
    @Min(0)
    @Documentation("tsload --max_ignored_rows <number>")
    private int maxIgnoredRows = 10;

    @Option
    @Documentation("tsload --bad_records_file <path_to_file>/<file_name>")
    @Pattern("^(\\/)?([^\\/\0|]+(\\/)?)+$")
    private String badRecordsFile;


    @Option
    @Documentation("tsload --date_format <date_formatmask> see: http://man7.org/linux/man-pages/man3/strptime.3.html")
    @Pattern("(%[A-Za-z]{1,2})[-/](%[A-Za-z]{1,2})[-/](%[A-Za-z]{1,2})")
    private String dateFormat = "%Y-%m-%d";

    @Option
    @Documentation("tsload --date_time_format <date_formatmask> <time_formatmask> see: http://man7.org/linux/man-pages/man3/strptime.3.html")
    @Pattern("(%[A-Za-z]{1,2})[-/](%[A-Za-z]{1,2})[-/](%[A-Za-z]{1,2})\\s(%[A-Za-z]{1,2}):(%[A-Za-z]{1,2}):(%[A-Za-z]{1,2})")
    private String dateTimeFormat = "%Y-%m-%d %H:%M:%S";

    @Option
    @Documentation("tsload --time_format <time_formatmask> see: http://man7.org/linux/man-pages/man3/strptime.3.html")
    @Pattern("(%[A-Za-z]{1,2}):(%[A-Za-z]{1,2}):(%[A-Za-z]{1,2})")
    private String timeFormat = "%H:%M:%S";

    @Option
    @Documentation("tsload --verbosity")
    @Min(0)
    @Max(3)
    private int verbosity = 0;

    @Option
    @Documentation("tsload --skip_second_fraction")
    private boolean skipSecondFraction = false;

    @Option
    @Documentation("tsload --date_converted_to_epoch")
    private boolean dateConvertedToEpoch = false;

    @Option
    @Documentation("tsload --boolean_representation")
    @Suggestable(value = "showBooleanRepresentation")
    private String booleanRepresentation = "T_F";



    public ThoughtSpotDataset getDataset() {
        return dataset;
    }

    public ThoughtSpotOutputConfiguration setDataset(ThoughtSpotDataset dataset) {
        this.dataset = dataset;
        return this;
    }

    public boolean getTruncate() {
        return truncate;
    }

    public ThoughtSpotOutputConfiguration setTruncate(boolean truncate)
    {
        this.truncate = truncate;
        return this;
    }

    public int getMaxIgnoredRows() {
        return maxIgnoredRows;
    }

    public ThoughtSpotOutputConfiguration setMaxIgnoredRows(int max_ignored_rows)
    {
        this.maxIgnoredRows = max_ignored_rows;
        return this;
    }

    public String getBadRecordsFile() {
        return this.badRecordsFile;
    }

    public ThoughtSpotOutputConfiguration setBadRecordsFile(String bad_records_file)
    {
        this.badRecordsFile = bad_records_file;
        return this;
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    public ThoughtSpotOutputConfiguration setDateFormat(String date_format)
    {
        this.dateFormat = date_format;
        return this;
    }

    public String getDateTimeFormat() {
        return this.dateTimeFormat;
    }

    public ThoughtSpotOutputConfiguration setDateTimeFormat(String date_time_format)
    {
        this.dateTimeFormat = date_time_format;
        return this;
    }

    public String getTimeFormat() {
        return this.timeFormat;
    }

    public ThoughtSpotOutputConfiguration setTimeFormat(String time_format)
    {
        this.timeFormat = time_format;
        return this;
    }

    public int getVerbosity() {
        return this.verbosity;
    }

    public ThoughtSpotOutputConfiguration setVerbosity(int verbosity)
    {
        this.verbosity = verbosity;
        return this;
    }

    public boolean getDateConvertedToEpoch() {
        return this.dateConvertedToEpoch;
    }

    public ThoughtSpotOutputConfiguration setDateConvertedToEpoch(boolean date_converted_to_epoch)
    {
        this.dateConvertedToEpoch = date_converted_to_epoch;
        return this;
    }

    public String getBooleanRepresentation() {
        return this.booleanRepresentation;
    }

    public ThoughtSpotOutputConfiguration setBooleanRepresentation(String boolean_representation)
    {
        this.booleanRepresentation = boolean_representation;
        return this;
    }

    public boolean skipSecondFraction() {
        return this.skipSecondFraction;
    }

    public ThoughtSpotOutputConfiguration skipSecondFraction(boolean skip_second_fraction)
    {
        this.skipSecondFraction = skip_second_fraction;
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