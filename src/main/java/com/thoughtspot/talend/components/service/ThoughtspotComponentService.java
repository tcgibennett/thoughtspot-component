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
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;

import com.thoughtspot.load_utility.TSLoadUtility;
import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;
import com.thoughtspot.talend.components.datastore.ThoughtSpotDataStore;



@Service
public class ThoughtspotComponentService {
	private static final transient Logger LOG = LoggerFactory.getLogger(ThoughtspotComponentService.class);
    // you can put logic here you can reuse in components


	@Suggestions("showBooleanRepresentation")
	public static SuggestionValues listBooleanRepresentation()
	{
		ArrayList<SuggestionValues.Item> items = new ArrayList<SuggestionValues.Item>();
		SuggestionValues values = new SuggestionValues();
		items.add(new SuggestionValues.Item("true_false","true_false"));
		items.add(new SuggestionValues.Item("1_0","1_0"));
		items.add(new SuggestionValues.Item("T_F","T_F"));
		items.add(new SuggestionValues.Item("Y_N","Y_N"));

		values.setItems(items);

		
		return values;
	}

	
    
    

    


    
	
	public LinkedHashMap<String, String> getTableColumns(ThoughtSpotDataset dataset) {
    	TSLoadUtility utility = TSLoadUtility.getInstance(dataset.getDatastore().getHost(), dataset.getDatastore().getPort(), dataset.getDatastore().getUsername(), dataset.getDatastore().getPassword());
    	LinkedHashMap<String, String> rs = null;
    	try {
    		utility.connect();
    		rs = utility.getTableColumns(dataset.getDatabase(), dataset.getSchema(), dataset.getTable());
    		utility.disconnect();
    	} catch(Exception e) {
    		LOG.error("TS:: " + e.getMessage());
    	}
    	
    	return rs;
    }
    
    
    
    private String getColumnNames(Schema schema)
    {
    	StringBuilder buff = new StringBuilder();
    	int i = 1;
		for (Schema.Entry entry : schema.getEntries())
		{
			if (i == schema.getEntries().size())
				buff.append(entry.getName());
			else
				buff.append(entry.getName()+",");
			i++;
		}
		
		return buff.toString();
    }
}