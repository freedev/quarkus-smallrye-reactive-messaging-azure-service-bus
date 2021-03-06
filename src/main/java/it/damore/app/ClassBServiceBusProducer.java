package it.damore.app;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import it.damore.models.ClassB;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ClassBServiceBusProducer extends AbstractAzureProducer<ClassB> {

    public ClassBServiceBusProducer(@ConfigProperty(name = "application.service-bus.class-b.topic") String outTopic) {
        super(outTopic);
    }
    
    @Incoming("from-processor-to-sb-producer")
    public void producer(ClassB msg) {
        log.info("sending " + msg);
            sendJsonMessages(List.of(msg));
    }

}
