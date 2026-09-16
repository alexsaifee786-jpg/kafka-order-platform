package com.aryan.kafka.inventoryservice.serializer;

import com.aryan.kafka.inventoryservice.event.InventoryReservedEvent;
import com.aryan.kafka.inventoryservice.event.InventoryReservationFailedEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

public class InventoryValueSerializer
        extends DelegatingByTypeSerializer {

    public InventoryValueSerializer() {
        super(createDelegates(), true);
    }

    private static Map<Class<?>, Serializer<?>> createDelegates() {
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();

        delegates.put(
                byte[].class,
                new ByteArraySerializer()
        );

        delegates.put(
                SpecificRecord.class,
                new KafkaAvroSerializer()
        );

        delegates.put(
                InventoryReservedEvent.class,
                new JsonSerializer<InventoryReservedEvent>()
        );

        delegates.put(
                InventoryReservationFailedEvent.class,
                new JsonSerializer<InventoryReservationFailedEvent>()
        );

        return delegates;
    }
}