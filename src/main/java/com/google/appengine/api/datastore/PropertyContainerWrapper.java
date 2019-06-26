package com.google.appengine.api.datastore;

import java.util.Map;

public abstract class PropertyContainerWrapper extends PropertyContainer
{
	private static final long serialVersionUID = 157842223244187697L;

	public PropertyContainerWrapper()
	{
		// TODO Auto-generated constructor stub
	}

	@Override
	public abstract Map<String, Object> getPropertyMap();

}
