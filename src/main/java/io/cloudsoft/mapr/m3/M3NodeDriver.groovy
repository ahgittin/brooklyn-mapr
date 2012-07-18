package io.cloudsoft.mapr.m3;

import java.util.List;

import io.cloudsoft.mapr.M3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import brooklyn.config.BrooklynLogging;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.lifecycle.StartStopDriver;
import brooklyn.entity.trait.Startable;
import brooklyn.location.Location;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.basic.jclouds.JcloudsLocation.JcloudsSshMachineLocation;

public class M3NodeDriver implements StartStopDriver {

    public static final Logger log = LoggerFactory.getLogger(M3NodeDriver.class);
    public static final Logger logSsh = LoggerFactory.getLogger(BrooklynLogging.SSH_IO);

    public static final String APT_GET_LINE = "deb http://package.mapr.com/releases/v1.2.7/ubuntu/ mapr optional";
    public static final String APT_GET_FILE = "/etc/apt/sources.list";
    public static final String DISKS_TXT_FQP = "/tmp/disks.txt";

    protected final AbstractM3Node entity;
    protected final SshMachineLocation machine;
    
    private boolean running = false;
    
    public M3NodeDriver(AbstractM3Node entity, SshMachineLocation machine) {
        this.entity = entity;
        this.machine = machine;
    }

    @Override public EntityLocal getEntity() { return entity; }
    @Override public Location getLocation() { return machine; }
    
    public void aptGetUpdate() {
        exec([
            "if [ ! -f ${APT_GET_FILE} ] ; then exit 44 ; fi",
            "if [ -z \"`grep '${APT_GET_LINE}' ${APT_GET_FILE}`\" ] ; then "+
                "sudo sh -c 'echo ${APT_GET_LINE} >> ${APT_GET_FILE}'"+
            " ; fi",
            "sudo apt-get update"
        ]);
    }
        
    public void aptGetInstall() {
        exec([ "sudo apt-get install -y --allow-unauthenticated "+entity.getAptPackagesToInstall().join(" ") ]);
    }
    
    public void configureMapR() {
        String masterHostname = entity.getConfig(M3.MASTER_HOSTNAME);
        String zkHostnames = entity.getConfig(M3.ZOOKEEPER_HOSTNAMES).join(",");
        exec([ "sudo /opt/mapr/server/configure.sh -C ${masterHostname} -Z ${zkHostnames}" ]);
    } 
    
    public void setupDisks() {
        //wait for machine to settle down... if it has trouble running script early
//        Thread.sleep(60*1000);
        
        DiskSetupSpec disks = entity.getConfig(AbstractM3Node.DISK_SETUP_SPEC);
        if (disks==null) throw new IllegalStateException("DISK_SETUP_SPEC is required for all nodes");
        
        log.info("${entity}: setting up disks");
        if (disks.getCommandsToRun()) {
            try {
                ((JcloudsSshMachineLocation)machine).execRemoteScript(disks.getCommandsToRun().toArray(new String[0]));
            } catch (Exception e) {
                log.warn("${entity}: error running custom commands for setting up disks; possibly a bug fixed in jclouds 1.4.1 (process will wait and script may run/finish normally): "+e);
                log.debug("${entity}: details of error running custom commands for setting up disks; possibly a bug fixed in jclouds 1.4.1: "+e, e);
                Thread.sleep(10*60*1000);
            }
        }
        machine.copyTo(new StringReader(disks.getPathsForDisksTxt().join("\n")), DISKS_TXT_FQP);
        exec([ "sudo /opt/mapr/server/disksetup -F ${DISKS_TXT_FQP}" ]);
        log.info("${entity}: disks set up, "+disks.getPathsForDisksTxt().join(" "));
    }

    public void startZookeeper() {
        Thread.sleep(10*1000);
        if (!entity.isZookeeper()) throw new IllegalStateException("${entity} is not a zookeeper node");
        log.info("${entity}: start zookeeper (${machine})");
        exec([ "sudo nohup /etc/init.d/mapr-zookeeper start" ]);
        entity.setAttribute(AbstractM3Node.ZOOKEEPER_UP, true);
    }
        
    public void startWarden() {
        log.info("${entity}: start warden");
        exec([ "sudo nohup /etc/init.d/mapr-warden start" ]);
        Thread.sleep(60*1000);
    }
        
    public void installJdk() {
        log.info("${entity}: installing JDK");
        // we use Open JDK, despite MapR warnings, because it is freely available
        // for Oracle JDK you will have to set up your own repo (and agree the license)
        // (they are working on changing that model but it will take a while)
        
        //this should work, in jclouds 1.4.1 or 1.5.0 (not in 1.4.0 because oracle have blocked download)
//        ExecResponse result = ((JcloudsSshMachineLocation)machine).submitRunScript(InstallJDK.fromURL()).get();
        
        //this works on ubuntu (surprising that jdk not in default repos!)
        exec([
            "sudo add-apt-repository ppa:dlecan/openjdk < /dev/null",
			// command above fails in ubuntu 12, but isn't needed there
            "sudo apt-get update",
            "sudo apt-get install -y --allow-unauthenticated openjdk-7-jdk"
        ]);
        log.info("${entity}: installed JDK");
    }
    
    /** does common setup up to the point where zookeeper is running;
     * after this, node1 does some more things, then other nodes result when node1 is ready
     */
    public void runMaprPhase1() {
        // useful to let machine quiet down, until jclouds 1.4.1 or 1.5.0 is released (not needed thereafter)
        Thread.sleep(60*1000);
        
        aptGetUpdate();
        aptGetInstall();
        configureMapR();
        setupDisks();
        if (entity.isZookeeper()) startZookeeper();
        else entity.setAttribute(AbstractM3Node.ZOOKEEPER_UP, false);
    }

    @Override
    public void start() {
        installJdk();
        runMaprPhase1();
        //wait for ZK to be running everywhere
        entity.getConfig(M3.ZOOKEEPER_READY);
        entity.runMaprPhase2();
        running = true;
        entity.setAttribute(Startable.SERVICE_UP, true);
    }

    @Override
    public void stop() {
        exec([ "sudo nohup /etc/init.d/mapr-warden stop" ]);
    }

    @Override
    public boolean isRunning() {
        // TODO this is a poor man's test ... exec 'mapr-warden status' ?
        return running;
    }

    @Override
    public void restart() {
        exec([ "sudo nohup /etc/init.d/mapr-warden restart" ]);
    }
    
    public int exec(List<String> commands) {
        int result = machine.execCommands(logPrefix: ""+entity.getId()+"@"+machine.getName(), "M3:"+entity, commands);
        if (result!=0)
            log.warn("FAILED (exit status ${result}) running "+entity+" "+commands+"; subsequent commands may not work");
        return result;
    }

}
