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
public class ClassBProducer extends AbstractAzureProducer<ClassB> {

    public ClassBProducer(@ConfigProperty(name = "application.service-bus.class-b.topic") String outTopic) {
        super(outTopic);
    }
    
    @Incoming("from-processor-to-sb-producer")
    public CompletionStage<Void> producer(List<ClassB> msg) {
        log.info("sending " + msg.size());
        return Uni.createFrom()
                .item(msg)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem()
                .invoke(list -> {
                    sendJsonMessages(list);
                })
                .onItem()
                .ignore()
                .andContinueWithNull()
                .subscribeAsCompletionStage();
    }

}
