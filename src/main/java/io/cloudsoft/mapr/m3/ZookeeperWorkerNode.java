package io.cloudsoft.mapr.m3;

import brooklyn.entity.Entity;
import com.google.common.collect.ImmutableList;
import io.cloudsoft.mapr.M3;

import java.util.List;
import java.util.Map;

public class ZookeeperWorkerNode extends AbstractM3Node {

   public ZookeeperWorkerNode(Entity owner) {
      super(owner);
   }

   public ZookeeperWorkerNode(Map properties, Entity owner) {
      super(properties, owner);
   }

   public List<String> getAptPackagesToInstall() {
      return ImmutableList.<String>builder().add("mapr-zookeeper").addAll(super.getAptPackagesToInstall()).build();
   }

   public boolean isZookeeper() { return true; }

   public void startServices() {
      log.info("ZookeeperWorkerNode node {} waiting for master", this);
      getConfig(M3.MASTER_UP);
      log.info("ZookeeperWorkerNode node {} detected master up", this);
   }

}
