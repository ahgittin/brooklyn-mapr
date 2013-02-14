package io.cloudsoft.mapr.m3

import java.util.List;
import java.util.Map;

import org.jclouds.compute.domain.OsFamily
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger
import org.slf4j.LoggerFactory


import brooklyn.entity.Entity
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.SoftwareProcessEntity
import brooklyn.entity.trait.Startable;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.NoMachinesAvailableException
import brooklyn.location.basic.SshMachineLocation
import brooklyn.location.basic.jclouds.templates.PortableTemplateBuilder

abstract class AbstractM3Node extends SoftwareProcessEntity implements Startable {

    public static final Logger log = LoggerFactory.getLogger(AbstractM3Node.class);

    public static BasicConfigKey<DiskSetupSpec> DISK_SETUP_SPEC = [ DiskSetupSpec, "mapr.node.disk.setup", "" ];
    public static final BasicAttributeSensor<Boolean> ZOOKEEPER_UP = [ Boolean, "mapr.zookeeper.serviceUp", "whether zookeeper has been started" ];
    
    public AbstractM3Node(Entity owner) { this([:], owner) }
    public AbstractM3Node(Map properties=[:], Entity owner=null) {
        super(properties, owner)

        setAttribute(SERVICE_UP, false)
        setAttribute(SERVICE_STATE, Lifecycle.CREATED)
    }
    
    public static BasicConfigKey<String> MAPR_USERNAME = [ String, "mapr.username", "initial user to create for mapr", "mapr" ];
    public static BasicConfigKey<String> MAPR_PASSWORD = [ String, "mapr.password", "initial password for initial user" ];

    public String getUser() { getConfig(MAPR_USERNAME) }
    public String getPassword() { getConfig(MAPR_PASSWORD) }
    
    public boolean isZookeeper() { return false; }
    
    public List<String> getAptPackagesToInstall() {
        List<String> result = [ "mapr-fileserver", "mapr-tasktracker" ];
        if (isZookeeper()) result << "mapr-zookeeper";
        return result;
    }

    @Override
    public Class<? extends M3NodeDriver> getDriverInterface() {
        return M3NodeDriver.class;
    }
        
    @Override
    public M3NodeDriver getDriver() {
        return (M3NodeDriver) super.getDriver();
    }
        
    protected Map<String,Object> getProvisioningFlags(MachineProvisioningLocation location) {
        obtainProvisioningFlags(location);
    }
    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = [:]; //super.obtainProvisioningFlags(location); 
        flags.templateBuilder = new PortableTemplateBuilder().
            osFamily(OsFamily.UBUNTU).osVersionMatches("11.04").os64Bit(true).
            minRam(2560);
        flags.userName = "ubuntu";
        flags.inboundPorts = 
            // from: http://www.mapr.com/doc/display/MapR/Ports+Used+by+MapR
            [ 22, 2048, 5660, 5181, 7221, 7222, 8080, 8443, 9001, 9997, 9998, 50030, 50060, 60000 ] +
            // 3888 discovered also to be needed, others included for good measure
            [ 2888, 3888 ]
            ;
        return flags;        
    }
    
    public abstract void runMaprPhase2();
    
}
