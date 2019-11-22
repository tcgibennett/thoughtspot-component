package com.thoughtspot.talend.components.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;



@Service
public class ThoughtspotComponentService {
	private static final transient Logger LOG = LoggerFactory.getLogger(ThoughtspotComponentService.class);
    // you can put logic here you can reuse in components


/*
	@DiscoverSchema(family = "ThoughtSpot",value = "discover")
    public Schema guessTableSchema(@Option final ThoughtSpotDataset dataset, final RecordBuilderFactory factory) {
		final Schema.Entry.Builder entryBuilder = factory.newEntryBuilder();
		final org.talend.sdk.component.api.record.Schema.Builder schemaBuilder = factory.newSchemaBuilder(Schema.Type.RECORD);
		
		Collection<Schema.Entry> entries = new ArrayList<Schema.Entry>();
		try {
			LinkedHashMap<String, String> rs = dataset.getTableColumns();
			for(String key : rs.keySet())
			{
				if (rs.get(key).equalsIgnoreCase("varchar"))
					schemaBuilder.withEntry(entryBuilder.withName(key).withType(Schema.Type.STRING).build());
				else if (rs.get(key).contains("int32"))
					schemaBuilder.withEntry(entryBuilder.withName(key).withType(Schema.Type.INT).build());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
		
        return schemaBuilder.build();
    }
*/
}