package it.damore.app;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.time.Duration;

public abstract class AbstractAzureConsumer<T> extends BaseClass {

    protected BroadcastProcessor<T> processor;
    protected ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient;
    protected String inTopicName;
    protected String subscriptionName;
    protected Integer prefetchCount = 0;

    Duration maxAutoLockRenewDuration;

    @Inject
    protected ServiceBusClientBuilder serviceBusClientBuilder;

//    ExecutorService executorService = Executors.newFixedThreadPool(4);

    protected AbstractAzureConsumer() {
        super();
    }

    public AbstractAzureConsumer(String inTopicName, String subscriptionName, BroadcastProcessor<T> processor) {
        super();
        this.inTopicName = inTopicName;
        this.subscriptionName = subscriptionName;
        this.processor = processor;

    }

    protected void onStart(@Observes StartupEvent ev) {

        serviceBusReceiverAsyncClient = serviceBusClientBuilder
                .transportType(AmqpTransportType.AMQP_WEB_SOCKETS)
                .receiver()
                .topicName(inTopicName)
                .subscriptionName(subscriptionName)
                .prefetchCount(prefetchCount)
                .maxAutoLockRenewDuration(this.maxAutoLockRenewDuration)
                .disableAutoComplete()
                .buildAsyncClient();

        Multi.createFrom()
                .publisher(serviceBusReceiverAsyncClient.receiveMessages())
//                .emitOn(executorService)
                .onItem()
                .transform(m -> this.consumeMessage(m))
                .subscribe()
                .with(o -> this.processReceivedMessage(o));


        log.info("onStart end");
    }

    public T consumeMessage(ServiceBusReceivedMessage message) {
//        log.infof("Received message from Azure ServiceBus: %s", message.getBody().toString());
        try {
            T azureMessage = parseReceivedMessage(message.getBody().toString());
            serviceBusReceiverAsyncClient.complete(message).subscribe();
            return azureMessage;
//            log.infof("Ack message sent on Azure Service Bus: %s", azureMessage);
        } catch (Exception e) {
            log.errorf("Error while processing message from Azure ServiceBus: %s", e.getMessage());
            log.errorf("Error while processing message from Azure ServiceBus: %s", message.getBody().toString());
            serviceBusReceiverAsyncClient.abandon(message).subscribe();
        }
        return null;
    }

    protected abstract T parseReceivedMessage(String msg) throws Exception;

    protected void processReceivedMessage(T msg) {
        processor.onNext(msg);
    }

}
