package io.cloudsoft.mapr.m3

import java.util.List;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.config.render.RendererHints;
import brooklyn.entity.Effector;
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.MethodEffector
import brooklyn.entity.basic.NamedParameter;
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.Location;

import groovy.transform.InheritConstructors;

@InheritConstructors
class MasterNode extends AbstractM3Node {

    public static final Logger log = LoggerFactory.getLogger(MasterNode.class);
    
    public static final BasicAttributeSensor<String> MAPR_URL = [ String, "mapr.url", "URL where MapR can be accessed" ];
    static {
        RendererHints.register(MAPR_URL, new RendererHints.NamedActionWithUrl("Open"));
    }
            
    public boolean isZookeeper() { return true; }
    
    public List<String> getAptPackagesToInstall() {
        [ "mapr-cldb", "mapr-jobtracker", "mapr-nfs", "mapr-webserver" ] + super.getAptPackagesToInstall();
    }

    public void startMasterServices() {
        // start the services -- no longer needed in v2
//        if (...VERSION.startsWith("v1."))
//            driver.exec([ "sudo /opt/mapr/bin/maprcli node services -nodes ${getAttribute(HOSTNAME)} -nfs start" ]);
    }    
    
    public void runMaprPhase2() {
        driver.startWarden();
        startMasterServices();
        
        // TODO this should happen on all nodes
        // (but isn't needed except for metrics)
        // since v2 seems this must be done after warden is started?
        driver.setupAdminUserMapr(getUser(), getPassword());
        
        // not sure this sleep is necessary
        Thread.sleep(10*1000);
        setAttribute(MAPR_URL, "https://${getAttribute(HOSTNAME)}:8443")
    }

    public void start(Collection<? extends Location> locations) {
        if (!getPassword())
            throw new IllegalArgumentException("configuration "+MAPR_PASSWORD.getName()+" must be specified");
        super.start(locations);
    }
    
}
