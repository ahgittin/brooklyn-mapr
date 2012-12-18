package io.cloudsoft.mapr;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Module;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.callables.RunScriptOnNode.Factory;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.scriptbuilder.domain.InterpretableStatement;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class JcloudsRunScriptOnNodeStatusNotAlwaysInitialized {

    private static final String IDENTITY = "AKIAI2SPATD5BUGMKDLQ";
    private static final String CREDENTIAL = "bujwmtQc5nXnAY3cYeA3c+DRaUavex2ZzpJZYTml";
    private static final String KEY_FILE =
            "/tmp/id_rsa";
//            System.getProperty("user.home")+"/.ssh/id_rsa";

   @Test
   public void testCanRunScript() throws IOException, RunNodesException, InterruptedException, ExecutionException {
      ComputeServiceContext context = ContextBuilder.newBuilder("aws-ec2")
              .modules(ImmutableSet.<Module>of(new SLF4JLoggingModule(), new SshjSshClientModule()))
              .credentials(IDENTITY, CREDENTIAL)
              .build(ComputeServiceContext.class);

      String user = "ubuntu";
      String privateKeyData = new String(Files.toByteArray(new File(KEY_FILE)));

      LoginCredentials credentials = LoginCredentials.fromCredentials(new Credentials(user, privateKeyData));


      ComputeService svc = context.getComputeService();

      NodeMetadata node = svc.getNodeMetadata("us-east-1/i-6aba1613");
      System.out.println("node: " + node);

      node = NodeMetadataBuilder.fromNodeMetadata(node).credentials(credentials).build();

      Factory runScriptFactory = context.utils().injector().getInstance(RunScriptOnNode.Factory.class);
      Statement script = new InterpretableStatement("echo hello going to dd",
              "ls /",
              "dd if=/dev/zero of=/mnt/mapr-storagefile bs=512M count=40",
              "echo done dd",
              "ls -al /mnt");
      ListenableFuture<ExecResponse> job = runScriptFactory.submit(node, script, new RunScriptOptions());
      System.out.println("waiting for " + job);
      ExecResponse response = job.get();
      System.out.println("got " + response);
   }

   public static void main(String[] args) throws IOException, RunNodesException, InterruptedException,
           ExecutionException {
      new JcloudsRunScriptOnNodeStatusNotAlwaysInitialized().testCanRunScript();
   }
}
