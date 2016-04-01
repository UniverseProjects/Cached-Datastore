package com.universeprojects.cacheddatastore;

import java.io.Serializable;
import java.util.Map;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

public class CachedEntity implements Cloneable,Serializable {
	private static final long serialVersionUID = 3034412029610092898L;
	private Entity entity;
	
	public CachedEntity(Key key)
	{
		this(new Entity(key));
	}
	
	public CachedEntity(String kind)
	{
		this(new Entity(kind));
	}
	
	public CachedEntity(String kind, Key parent)
	{
		this(new Entity(kind, parent));
	}
	
	public CachedEntity(String kind, long id)
	{
		this(new Entity(kind, id));
	}
	
	public CachedEntity(String kind, long id, Key parent)
	{
		this(new Entity(kind, id, parent));
	}
	
	public CachedEntity(String kind, String keyName)
	{
		this(new Entity(kind, keyName));
	}
	
	public CachedEntity(String kind, String keyName, Key parent)
	{
		this(new Entity(kind, keyName, parent));
	}
	
	
	
	private CachedEntity(Entity entity) {
		this.entity = entity;
	}
	
	
	/**
	 * Since this class is meant to make Entity more compatible with
	 * the schema system for the GEF, it is recommended to just use
	 * CachedEntity for normal Entity interactions. 
	 * 
	 * If you must, use this call wisely.
	 * 
	 * @return
	 */
	public Entity getEntity()
	{
		return entity;
	}
	
	public CachedEntity clone()
	{
		return new CachedEntity(entity.clone());
	}
	
	public boolean equals(Object object)
	{
		if (object==null) return false;
		if (object==this)
			return true;
		if (object instanceof CachedEntity)
			return this.entity.equals(((CachedEntity)object).entity);
		else if (object instanceof Entity)
			return this.entity.equals(object);
		else
			return false;
	}
	
	public String getAppId()
	{
		return entity.getAppId();
	}
	
	public Key getKey()
	{
		return entity.getKey();
	}
	
	public String getKind()
	{
		return entity.getKind();
	}
	
	public Long getId()
	{
		return entity.getKey().getId();
	}
	
	public String getNamespace()
	{
		return entity.getNamespace();
	}
	
	public Key getParent()
	{
		return entity.getParent();
	}
	
	public int hashCode()
	{
		return entity.hashCode();
	}
	
	public String toString()
	{
		return entity.toString();
	}
	
	public Map<String, Object> getProperties()
	{
		return entity.getProperties();
	}
	
	public Object getProperty(String propertyName)
	{
		return entity.getProperty(propertyName);
	}
	
	public boolean hasProperty(String propertyName)
	{
		return entity.hasProperty(propertyName);
	}
	
	public boolean isUnindexedProperty(String propertyName)
	{
		return entity.isUnindexedProperty(propertyName);
	}
	
	public void removeProperty(String propertyName)
	{
		entity.removeProperty(propertyName);
	}
	
	
	public void setProperty(String propertyName, Object value)
	{
		// Determine if this field should be indexed or not depending on the schema
		CachedSchema schema = SchemaInitializer.getSchema();

		Boolean unindexed=false;

		if (schema!=null)
		{
			unindexed = schema.isFieldUnindexed(entity.getKind(), propertyName);
			if (unindexed==null) unindexed = false;

			// Here is a little nice thing to convert a string to a Text if that is the setting in the schema field
//			if (sField.getType().equals(SchemaFieldType.Text) && value instanceof String)
//				value = new Text((String)value);
		}

		
		
		if (unindexed)
			entity.setUnindexedProperty(propertyName, value);
		else
			entity.setProperty(propertyName, value);
	}
	
	public static CachedEntity wrap(Entity obj)
	{
		if (obj==null) return null;
		return new CachedEntity((Entity)obj);
	}

	public void setPropertyManually(String propertyName, Object value)
	{
		entity.setProperty(propertyName, value);
	}
	
	public void setUnindexedPropertyManually(String propertyName, Object value)
	{
		entity.setUnindexedProperty(propertyName, value);
	}
	
	

}
