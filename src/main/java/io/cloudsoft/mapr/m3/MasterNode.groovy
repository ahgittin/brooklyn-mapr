package io.cloudsoft.mapr.m3

import java.util.List;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.Effector;
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.MethodEffector
import brooklyn.entity.basic.NamedParameter;
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.DependentConfiguration;

import groovy.transform.InheritConstructors;

@InheritConstructors
class MasterNode extends AbstractM3Node {

    public static final Logger log = LoggerFactory.getLogger(MasterNode.class);
    
    public static final BasicAttributeSensor<String> LICENSE_APPROVED = [ String, "mapr.master.license", "this attribute is set when the license is approved (manually)" ];
    public static final Effector<Void> SET_LICENSE_APPROVED = new MethodEffector(MasterNode.&setLicenseApproved);
    
    public boolean isZookeeper() { return true; }
    
    public List<String> getAptPackagesToInstall() {
        [ "mapr-cldb", "mapr-jobtracker", "mapr-nfs", "mapr-webserver" ] + super.getAptPackagesToInstall();
    }

    // TODO config param?  note, if this is not 'ubuntu', we have to create the user; see jclouds AdminAccess
    public String getUser() { "ubuntu" }
    public String getPassword() { "m4pr" }
    
    public void setupAdminUser(String user, String password) {
        //    On node 1, give full permission to the chosen administrative user using the following command:
        //    (and set a passwd)
        driver.exec([
            "echo \"${password}\n${password}\" | sudo passwd ${user}",
            "sudo /opt/mapr/bin/maprcli acl edit -type cluster -user ${user}:fc" ]);
    }
    
    public void waitForLicense() {
        // MANUALLY: accept the license
        //    https://<node 1>:8443  -->  accept agreement, login, add license key
        log.info("${this} waiting for MapR LICENSE"+"""
**********************************************************************
* LICENSE must be accepted manually at:
*   https://${getAttribute(HOSTNAME)}:8443
* THEN invoke effector  setLicenseApproved true  at:
*   http://localhost:8081
**********************************************************************""");
        execution.submit(DependentConfiguration.attributeWhenReady(this, LICENSE_APPROVED)).get();
        log.info("MapR LICENSE accepted, proceeding");
    }

    public void startMasterServices() {
        // start the services
        driver.exec([ "sudo /opt/mapr/bin/maprcli node services -nodes ${getAttribute(HOSTNAME)} -nfs start" ]);
    }    
    
    public void runMaprPhase2() {
        driver.startWarden();
        setupAdminUser(user, password);
        waitForLicense();
        startMasterServices();
        
        // not sure this sleep is necessary, but seems safer...
        Thread.sleep(10*1000);
    }

    @Description("Sets an attribute on the entity to indicate that the license has been approved")
    public void setLicenseApproved(@NamedParameter("text") String text) {
        log.info("MapR master {} got license approved invoked with: {}", this, text);
        setAttribute(LICENSE_APPROVED, text);
    }
    
}
