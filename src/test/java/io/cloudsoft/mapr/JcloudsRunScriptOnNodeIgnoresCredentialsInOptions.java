package io.cloudsoft.mapr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.callables.RunScriptOnNode.Factory;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.Module;

public class JcloudsRunScriptOnNodeIgnoresCredentialsInOptions {

    private static final String IDENTITY = "AKIAI2SPATD5BUGMKDLQ";
    private static final String CREDENTIAL = "bujwmtQc5nXnAY3cYeA3c+DRaUavex2ZzpJZYTml";
    private static final String KEY_FILE = System.getProperty("user.home")+"/.ssh/id_rsa";
    
    @Test
    public void testCanRunScript() throws IOException, RunNodesException, InterruptedException, ExecutionException {
        ComputeServiceContext context = new ComputeServiceContextFactory().createContext(
                "aws-ec2",
                IDENTITY, 
                CREDENTIAL,
                ImmutableSet.<Module> of(new SLF4JLoggingModule(), new SshjSshClientModule()),
                new Properties());

        String user = "ubuntu";
        String privateKeyData = new String(Files.toByteArray(new File(KEY_FILE)));
        String publicKeyData = new String(Files.toByteArray(new File(KEY_FILE+".pub")));
        
        LoginCredentials credentials = LoginCredentials.fromCredentials(new Credentials(user, privateKeyData));

        ComputeService svc = context.getComputeService();
        TemplateBuilder templateBuilder = svc.templateBuilder();
        templateBuilder.osFamily(OsFamily.UBUNTU);
        Template template = templateBuilder.build();
        template.getOptions().authorizePublicKey(publicKeyData);
        
        Set<? extends NodeMetadata> nodes = svc.createNodesInGroup("jclouds-bug-test_"+System.getProperty("user.name"), 1, template);
        NodeMetadata node = Iterables.getOnlyElement(nodes);

        List<String> failures = new ArrayList<String>();
        Statement statement = Statements.newStatementList(Statements.exec("date"));
        
        try {
            ExecResponse response = svc.runScriptOnNode(node.getId(), statement);
            Assert.assertEquals(response.getExitStatus(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            failures.add("attempt 1: "+e);
        }

        try {
            ExecResponse response = svc.runScriptOnNode(node.getId(), statement, 
                    RunScriptOptions.Builder.overrideLoginCredentials(credentials));
            Assert.assertEquals(response.getExitStatus(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            failures.add("attempt 2: "+e);
        }

        try {
            Factory runScriptFactory = context.utils().injector().getInstance(RunScriptOnNode.Factory.class);
            ExecResponse response = runScriptFactory.submit(node, statement, 
                    RunScriptOptions.Builder.overrideLoginCredentials(credentials)).get();
            Assert.assertEquals(response.getExitStatus(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            failures.add("attempt 3: "+e);
        }
        
        if (!failures.isEmpty())
            Assert.fail(""+failures);
    }
    
    public static void main(String[] args) throws IOException, RunNodesException, InterruptedException, ExecutionException {
        new JcloudsRunScriptOnNodeIgnoresCredentialsInOptions().testCanRunScript();
    }
}
