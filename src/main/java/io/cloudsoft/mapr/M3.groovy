package io.cloudsoft.mapr

import java.util.Collection;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Predicate

import groovy.transform.InheritConstructors;
import io.cloudsoft.mapr.m3.AbstractM3Node
import io.cloudsoft.mapr.m3.MasterNode
import io.cloudsoft.mapr.m3.WorkerNode
import io.cloudsoft.mapr.m3.ZookeeperWorkerNode
import brooklyn.entity.Entity
import brooklyn.entity.basic.AbstractEntity
import brooklyn.entity.basic.BasicConfigurableEntityFactory
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.trait.Startable
import brooklyn.entity.trait.StartableMethods;
import brooklyn.event.Sensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.Location;
import brooklyn.management.Task
import brooklyn.util.task.ParallelTask

import static brooklyn.event.basic.DependentConfiguration.*;

@InheritConstructors
public class M3 extends AbstractEntity implements Startable {

    public static final Logger log = LoggerFactory.getLogger(M3.class);
    
    public static BasicConfigKey<String> MASTER_HOSTNAME = [ String, "mapr.master.hostname", "" ];
    public static BasicConfigKey<List<String>> ZOOKEEPER_HOSTNAMES = [ List, "mapr.zk.hostnames", "" ];
    public static BasicConfigKey<List<Boolean>> ZOOKEEPER_READY = [ List, "mapr.zk.ready", "" ];
    
    public static BasicConfigKey<Boolean> MASTER_UP = [ Boolean, "mapr.master.serviceUp", "" ];
    
    MasterNode master = new MasterNode(this, name: "node1 (master)");
    ZookeeperWorkerNode zk1 = new ZookeeperWorkerNode(this, name: "node2 (zk+worker)");
    ZookeeperWorkerNode zk2 = new ZookeeperWorkerNode(this, name: "node3 (zk+worker)");    
    
    DynamicCluster workers = new DynamicCluster(this, factory: new BasicConfigurableEntityFactory(WorkerNode), 
        initialSize: 2);

    @Override public void start(Collection<? extends Location> locations) { StartableMethods.start(this, locations); }
    @Override public void stop() { StartableMethods.stop(this); }
    @Override public void restart() { StartableMethods.restart(this); }
    
    {
        setConfig(MASTER_UP, DependentConfiguration.attributeWhenReady(master, MasterNode.SERVICE_UP));
        
        setConfig(MASTER_HOSTNAME, DependentConfiguration.attributeWhenReady(master, MasterNode.HOSTNAME));
        
        final def zookeeperNodes = ownedChildren.findAll({ (it in AbstractM3Node) && (it.isZookeeper()) });
        setConfig(ZOOKEEPER_HOSTNAMES, listAttributesWhenReady(AbstractM3Node.HOSTNAME, zookeeperNodes));
        setConfig(ZOOKEEPER_READY, listAttributesWhenReady(AbstractM3Node.ZOOKEEPER_UP, zookeeperNodes));
    }
    
    // TODO promote this to brooklyn DependentConfiguration
    public static <T> Task<List<T>> listAttributesWhenReady(Sensor<T> sensor, Iterable<Entity> entities, Predicate<T> readiness = null) {
        new ParallelTask(entities.collect({ attributeWhenReady(it, sensor, readiness) }) );
    }
    
}
