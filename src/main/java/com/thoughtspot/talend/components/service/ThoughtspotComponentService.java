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
import com.thoughtspot.talend.components.dataset.ThoughtSpotDataset;



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
}