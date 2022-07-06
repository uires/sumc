package br.com.sumc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    public static final String TYPE_CONFIG = "br.com.sumc.type_config";
    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName = String.valueOf(configs.get(TYPE_CONFIG));
        try {
            this.type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException exception) {
            throw new RuntimeException("Invalid type, class doesn't exist in classpath", exception);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return this.gson.fromJson(new String(bytes), this.type);
    }
}
