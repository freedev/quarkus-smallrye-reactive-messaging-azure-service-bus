package it.damore.config;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class AzureServiceBusConfiguration {

    private final String connectionString;

    public AzureServiceBusConfiguration(@ConfigProperty(name="application.service-bus.connection-string") String connectionString){
        this.connectionString = connectionString;
    }

    @Produces
    public ServiceBusClientBuilder sharedConnectionBuilder(){
        return new ServiceBusClientBuilder().connectionString(connectionString);
    }

}
