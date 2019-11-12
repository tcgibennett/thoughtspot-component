package com.thoughtspot.talend.components.service;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import com.thoughtspot.talend.components.datastore.ThoughtSpotDataStore;

import static java.util.Collections.singletonList;

import java.sql.Connection;


@Service
public class DatastoreValidationService {
	
	
	
	
	@HealthCheck(value = "DatastoreConnection")
	public HealthCheckStatus healthCheck(@Option ThoughtSpotDataStore conn)
	{
			if (conn.getConnection())
				return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Success");
			else
				return new HealthCheckStatus(HealthCheckStatus.Status.KO, "Failed");
	}
	
	
	
	@Suggestions("showTables")
	public static SuggestionValues listTables(@Option ThoughtSpotDataStore datastore)
	{
		
		SuggestionValues values = new SuggestionValues();
		values.setItems(datastore.getTables());
		
		return values;
	}
}
