package br.com.sumc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer<T> implements Serializer<T> {
    private final Gson gsonBuilder = new GsonBuilder().create();

    @Override
    public byte[] serialize(String s, T generic) {
        return this.gsonBuilder.toJson(generic).getBytes();
    }
}
