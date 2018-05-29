package com.universeprojects.cacheddatastore;

public class SchemaInitializer {
	private static SchemaProvider schemaProvider;

	public static void initializeSchema(final CachedSchema newSchema)
	{
		initializeSchemaProvider(new SchemaProvider() {
			@Override
			public CachedSchema getSchema() {
				return newSchema;
			}
		});
	}

	public static void initializeSchemaProvider(final SchemaProvider schemaProvider)
	{
		SchemaInitializer.schemaProvider = schemaProvider;
	}
	
	public static CachedSchema getSchema()
	{
		return schemaProvider.getSchema();
	}
}
