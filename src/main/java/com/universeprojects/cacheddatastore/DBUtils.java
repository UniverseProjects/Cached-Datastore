package com.universeprojects.cacheddatastore;

import com.google.appengine.api.datastore.Text;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public final class DBUtils {
    static final Base64 base64 = new Base64();

    public static String serializeObjectToString(Object object) {
        if (object == null) {
            return null;
        }
        try (
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(arrayOutputStream);) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
            return new String(base64.encode(arrayOutputStream.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object deserializeObjectFromString(String objectString) throws Exception {
        if (objectString == null) {
            return null;
        }
        try (
            ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(base64.decode(objectString));
            ObjectInputStream objectInputStream = new ObjectInputStream(arrayInputStream)) {
            return objectInputStream.readObject();
        } catch (IOException e) {
            throw new Exception(e);
        } catch (ClassNotFoundException e) {
            throw new Exception(e);
        }
    }

    public static String textToString(Object value) {
        if (value instanceof Text) {
            return ((Text) value).getValue();
        } else if (value instanceof String) {
            return (String) value;
        } else if (value == null) {
            return null;
        } else {
            throw new RuntimeException("Invalid type " + value.getClass().getSimpleName() + ". Expected Text or String.");
        }
    }

    public static int getEntitySize(CachedEntity entity) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(entity.getEntity());
            oos.close();
        } catch (IOException e) {
            return -1;
        }
        return Base64.encodeBase64String(baos.toByteArray()).length();
    }

    public static Text toText(String string) {
        if (string == null) {
            return null;
        } else {
            return new Text(string);
        }
    }
}
