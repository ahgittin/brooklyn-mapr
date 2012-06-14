package io.cloudsoft.mapr.m3

import org.slf4j.Logger;
import org.slf4j.LoggerFactory

import io.cloudsoft.mapr.M3
import groovy.transform.InheritConstructors;

@InheritConstructors
class WorkerNode extends AbstractM3Node {

    public static final Logger log = LoggerFactory.getLogger(WorkerNode.class);
    
    public void runMaprPhase2() {
        log.info("MapR node {} waiting for master", this);
        getConfig(M3.MASTER_UP);
        log.info("MapR node {} detected master up, proceeding to start warden", this);
        driver.startWarden();
    }
    
}
