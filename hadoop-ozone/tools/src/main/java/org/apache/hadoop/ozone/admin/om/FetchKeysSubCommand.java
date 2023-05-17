package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin om fetch-key command.
 */
@CommandLine.Command(
    name = "fetch",
    description = "CLI command to fetch latest keys",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class FetchKeysSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  private OzoneManagerProtocol ozoneManagerClient;

  @Override
  public Void call() throws Exception {

    ozoneManagerClient = parent.createOmClient(omServiceId);
    ManagedSecretKey managedSecretKey =
        ozoneManagerClient.getCurrentSecretKey();
    System.out.println("Done!");
    return null;
  }
}

