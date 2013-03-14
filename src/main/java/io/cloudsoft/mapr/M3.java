package io.cloudsoft.mapr;

import brooklyn.enricher.basic.SensorPropagatingEnricher;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.AbstractEntity;
import brooklyn.entity.basic.BasicConfigurableEntityFactory;
import brooklyn.entity.group.Cluster;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.trait.Startable;
import brooklyn.entity.trait.StartableMethods;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.Location;
import brooklyn.policy.autoscaling.AutoScalerPolicy;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.cloudsoft.mapr.m3.AbstractM3Node;
import io.cloudsoft.mapr.m3.MasterNode;
import io.cloudsoft.mapr.m3.WorkerNode;
import io.cloudsoft.mapr.m3.ZookeeperWorkerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Maps.newLinkedHashMap;

public class M3 extends AbstractEntity implements Startable {

   public M3(Entity parent) {
      super(parent);
   }

   public M3(Map flags, Entity parent) {
      super(flags, parent);
   }

   public static final Logger log = LoggerFactory.getLogger(M3.class);

   public static BasicConfigKey<String> MASTER_HOSTNAME = new BasicConfigKey<String>(String.class,
           "mapr.master.hostname", "");

   /**
    * hostnames of all machines expected to run zookeeper
    */
   public static BasicConfigKey<List<String>> ZOOKEEPER_HOSTNAMES = new BasicConfigKey(List.class,
           "mapr.zk.hostnames", "");
   /**
    * configuration is set when all expected zookeepers have started the zookeeper process
    */
   public static BasicConfigKey<List<Boolean>> ZOOKEEPER_READY = new BasicConfigKey(List.class, "mapr.zk.ready", "");

   /**
    * configuration is set when the master node has come up (license approved etc)
    */
   public static BasicConfigKey<Boolean> MASTER_UP = new BasicConfigKey<Boolean>(Boolean.class,
           "mapr.master.serviceUp", "");

   // The DB master
   MasterNode master = new MasterNode(newLinkedHashMap(of("name", "node1 (master)")), this);

   // The zookeeper nodes
   ZookeeperWorkerNode zk1 = new ZookeeperWorkerNode(newLinkedHashMap(of("name", "node2")), this);
   ZookeeperWorkerNode zk2 = new ZookeeperWorkerNode(newLinkedHashMap(of("name", "node3")), this);

   // The Dynamic cluster
   DynamicCluster workers = new DynamicCluster(newLinkedHashMap(of(
           "factory", new BasicConfigurableEntityFactory(WorkerNode.class),
           "initialSize", 2)),
           this);

   {

      workers.addPolicy(AutoScalerPolicy.builder()
              .metric(MasterNode.CLUSTER_USED_DFS_PERCENT)
              .entityWithMetric(master)
              .sizeRange(2, 5)
              .metricRange(20.0, 80.0)
              .build());

      workers.setConfig(Cluster.INITIAL_SIZE, 2);


      setConfig(MASTER_UP, DependentConfiguration.attributeWhenReady(master, MasterNode.SERVICE_UP));
      setConfig(MASTER_HOSTNAME, DependentConfiguration.attributeWhenReady(master, MasterNode.SUBNET_HOSTNAME));

      Iterable<Entity> zookeeperNodes = Iterables.filter(getOwnedChildren(), new Predicate<Entity>() {
         @Override
         public boolean apply(@Nullable Entity input) {
            return AbstractM3Node.class.isAssignableFrom(input.getClass()) && ((AbstractM3Node) input).isZookeeper();
         }
      });

      setConfig(ZOOKEEPER_HOSTNAMES, DependentConfiguration.listAttributesWhenReady(AbstractM3Node.SUBNET_HOSTNAME,
              zookeeperNodes));
      setConfig(ZOOKEEPER_READY, DependentConfiguration.listAttributesWhenReady(AbstractM3Node.ZOOKEEPER_UP,
              zookeeperNodes));

      SensorPropagatingEnricher.newInstanceListeningTo(master, MasterNode.MAPR_URL).addToEntityAndEmitAll(this);
   }


   @Override
   public void start(Collection<? extends Location> locations) { StartableMethods.start(this, locations); }

   @Override
   public void stop() { StartableMethods.stop(this); }

   @Override
   public void restart() { StartableMethods.restart(this); }

}
