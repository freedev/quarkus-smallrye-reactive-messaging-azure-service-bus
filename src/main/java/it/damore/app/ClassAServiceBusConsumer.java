package it.damore.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import it.damore.models.ClassA;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ClassAServiceBusConsumer extends AbstractAzureConsumer<ClassA> {

    @Inject
    protected ObjectMapper objectMapper;
    @Inject
    public ClassAServiceBusConsumer(@ConfigProperty(name = "application.service-bus.class-a.topic") String inTopic,
                                    @ConfigProperty(name = "application.service-bus.class-a.subscription") String sub) {
        super(inTopic, sub, BroadcastProcessor.create());
        prefetchCount = 10;
    }

    @Override
    protected ClassA parseReceivedMessage(String msg) throws Exception {
        log.infof("received message {}", msg);
        return objectMapper.readValue(msg, ClassA.class);
    }

    @Outgoing("from-sb-consumer-to-consumer1")
    public Multi<ClassA> createStream() {
        log.infof("createStream called only once at assemblytime");
        return Multi.createFrom().publisher(processor);
    }

}
