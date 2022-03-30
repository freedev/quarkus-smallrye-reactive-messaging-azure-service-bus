package it.damore.app;

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
    private boolean disableAutocomplete = true;

    @Inject
    protected ServiceBusClientBuilder serviceBusClientBuilder;

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
//                    .transportType(AmqpTransportType.AMQP.AMQP_WEB_SOCKETS)
                .receiver()
                .topicName(inTopicName)
                .subscriptionName(subscriptionName)
                .prefetchCount(prefetchCount)
                .maxAutoLockRenewDuration(Duration.ofMinutes(1))
                .disableAutoComplete()
                .buildAsyncClient();

//            if (disableAutocomplete) {
//                this.consumer = serviceBusProcessorClientBuilder.disableAutoComplete();
//            } else {
//                this.consumer = serviceBusProcessorClientBuilder.buildProcessorClient();
//            }

        Multi.createFrom()
                .publisher(serviceBusReceiverAsyncClient.receiveMessages())
                .onItem()
                .transform(m -> this.consumeMessage(m))
                .subscribe()
                .with(o -> this.processReceivedMessage(o));


        log.info("onStart end");
    }

    public T consumeMessage(ServiceBusReceivedMessage message) {
        try {
            T azureMessage = parseReceivedMessage(message.getBody().toString());
            serviceBusReceiverAsyncClient.complete(message).subscribe();
            return azureMessage;
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
