package it.damore.app;

import org.jboss.logging.Logger;

public abstract class BaseClass {

    protected final Logger log;

    protected BaseClass() {
        this.log = Logger.getLogger(getClass());
    }
    
}
