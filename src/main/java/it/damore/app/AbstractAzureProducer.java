package it.damore.app;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.List;

public abstract class AbstractAzureProducer<T> extends BaseClass {

    protected ServiceBusSenderClient producer;
    private String outTopicName;

    @Inject
    protected ObjectMapper objectMapper;

    @Inject
    protected ServiceBusClientBuilder serviceBusClientBuilder;

    protected AbstractAzureProducer() {
        super();
    }

    public AbstractAzureProducer(String outTopicName) {
        super();
        this.outTopicName = outTopicName;
    }

    protected void onStart(@Observes StartupEvent ev) {

        producer = serviceBusClientBuilder
                .transportType(AmqpTransportType.AMQP.AMQP_WEB_SOCKETS)
                .sender()
                .topicName(outTopicName)
                .buildClient();

    }

    protected void sendMessage(String msg, String contentType) {
        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(msg);
        serviceBusMessage.setContentType(contentType);
        producer.sendMessage(serviceBusMessage);
    }

    protected void sendJsonMessage(Object msg) {
        try {
            ServiceBusMessage serviceBusMessage = new ServiceBusMessage(objectMapper.writeValueAsString(msg));
            serviceBusMessage.setContentType("application/json");
            producer.sendMessage(serviceBusMessage);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
    }

    protected void sendJsonMessages(List list) {
        ServiceBusMessageBatch messageBatch = producer.createMessageBatch();
        for (Object item : list) {
            try {
                String jsonMessage = objectMapper.writeValueAsString(item);
                ServiceBusMessage message = new ServiceBusMessage(jsonMessage);
                message.setContentType("application/json");
                try {
                    boolean isAdded = messageBatch.tryAddMessage(message);
                    if (!isAdded) {
                        if (messageBatch.getCount() == 0) {
                            throw new RuntimeException("message too big");
                        }
                        log.info("sending batch of " + messageBatch.getCount());
                        producer.sendMessages(messageBatch);
                        messageBatch = producer.createMessageBatch();
                        isAdded = messageBatch.tryAddMessage(message);
                        if (!isAdded) {
                            throw new RuntimeException("message too big");
                        }
                    }
                } catch (Exception e1) {
                    log.error(e1.getMessage());
                }
            } catch (Exception e1) {
                log.error(e1.getMessage());
            }
        }
        if (messageBatch.getCount() > 0) {
            log.info("sending batch of " + messageBatch.getCount());
            producer.sendMessages(messageBatch);
        }
    }

}
