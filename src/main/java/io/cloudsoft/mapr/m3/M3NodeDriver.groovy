package io.cloudsoft.mapr.m3;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.Map;

import io.cloudsoft.mapr.M3

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

import brooklyn.config.BrooklynLogging;
import brooklyn.entity.basic.EntityLocal
import brooklyn.entity.basic.lifecycle.StartStopDriver
import brooklyn.entity.trait.Startable;
import brooklyn.location.Location
import brooklyn.location.basic.SshMachineLocation
import brooklyn.location.basic.jclouds.JcloudsLocation.JcloudsSshMachineLocation
import brooklyn.util.internal.StreamGobbler;

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
        if (disks.getCommandsToRun()) 
            ((JcloudsSshMachineLocation)machine).execRemoteScript(disks.getCommandsToRun().toArray(new String[0]));
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
        //this doesn't work -- lacks credential:
//        if (machine in JcloudsSshMachineLocation) {
//            ((JcloudsSshMachineLocation)machine).parent.getComputeService().runScriptOnNode(
//                ((JcloudsSshMachineLocation)machine).node.getId(),
//                InstallJDK.fromURL()
//            );
//        }
        
        //this should work, but not in 1.4.0 because oracle have blocked download (fixed in head 1.4.1 and 1.5.0)
//        ExecResponse result = ((JcloudsSshMachineLocation)machine).submitRunScript(InstallJDK.fromURL()).get();
        
        //this works on ubuntu (surprising that jdk not in default repos!)
        exec([
            "sudo add-apt-repository ppa:dlecan/openjdk",
            "sudo apt-get update",
            "sudo apt-get install -y --allow-unauthenticated openjdk-7-jdk"
        ]);
        log.info("${entity}: installed JDK");
    }
    
    /** does common setup up to the point where zookeeper is running;
     * after this, node1 does some more things, then other nodes result when node1 is ready
     */
    public void runMaprPhase1() {
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
