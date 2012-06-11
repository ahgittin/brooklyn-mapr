package io.cloudsoft.mapr.m3

import groovy.transform.InheritConstructors;

@InheritConstructors
class ZookeeperWorkerNode extends WorkerNode {

    public boolean isZookeeper() { return true; }
    
}
