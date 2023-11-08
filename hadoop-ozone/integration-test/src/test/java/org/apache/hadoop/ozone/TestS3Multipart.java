package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadResponse;
import org.apache.hadoop.ozone.s3.endpoint.MultipartUploadInitiateResponse;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class TestS3Multipart {

  public static final Logger LOG = LoggerFactory.getLogger(TestS3Multipart.class);
  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static File workDir;
  private static String clusterId;
  private static String scmId;
  private static String omServiceId;
  private static String scmServiceId;
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;

  private static ContainerRequestContext context;

  private static final ObjectEndpoint REST = new ObjectEndpoint();

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    ExitUtils.disableSystemExit();

    workDir =
        GenericTestUtils.getTestDir(TestS3Multipart.class.getSimpleName());
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test";
    scmServiceId = "scm-service-test";

    startCluster();
    client = cluster.newClient();
    ozoneAdmin = new OzoneAdmin(conf);
    client.getObjectStore().createS3Bucket("buck1");

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    context = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(context.getUriInfo()).thenReturn(Mockito.mock(UriInfo.class));
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(new MultivaluedHashMap<>());

    REST.setHeaders(headers);
    REST.setClient(client);
    REST.setOzoneConfiguration(conf);
    REST.setContext(context);
  }

  private static void startCluster()
      throws IOException, TimeoutException, InterruptedException {
    OzoneManager.setTestSecureOmFlag(true);
    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setSCMServiceId(scmServiceId)
        .setOMServiceId(omServiceId)
        .setScmId(scmId)
        .setNumDatanodes(3)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(3);

    cluster = (MiniOzoneHAClusterImpl) builder.build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void stop() {
    IOUtils.close(LOG, client);
    if (cluster != null) {
      cluster.stop();
    }
    DefaultConfigManager.clearDefaultConfigs();
  }

  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
    Response response = REST.initializeMultipartUpload("buck1",
        key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();
    assertEquals(200, response.getStatus());
    return uploadID;
  }

  private CompleteMultipartUploadRequest.Part uploadPart(String key, String uploadID, int partNumber, String
      content) throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = REST.put("buck1", key, content.length(),
        partNumber, uploadID, body);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getHeaderString("ETag"));
    CompleteMultipartUploadRequest.Part
        part = new CompleteMultipartUploadRequest.Part();
    part.seteTag(response.getHeaderString("ETag"));
    part.setPartNumber(partNumber);
    return part;
  }

  private void completeMultipartUpload(String key,
                                       CompleteMultipartUploadRequest completeMultipartUploadRequest,
                                       String uploadID) throws IOException, OS3Exception {
    Response response = REST.completeMultipartUpload("buck1", key,
        uploadID, completeMultipartUploadRequest);

    assertEquals(200, response.getStatus());

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        (CompleteMultipartUploadResponse) response.getEntity();

    assertEquals("buck1",
        completeMultipartUploadResponse.getBucket());
    assertEquals(key, completeMultipartUploadResponse.getKey());
    assertEquals("buck1",
        completeMultipartUploadResponse.getLocation());
    assertNotNull(completeMultipartUploadResponse.getETag());
  }

  private void getObjectMultipart(String key, int partNumber) throws IOException, OS3Exception {
    Response response = REST.get("buck1", key, partNumber, null, 100, null);
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testMultipart() throws Exception {

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(OzoneConsts.KEY);

    List<CompleteMultipartUploadRequest.Part> partsList = new ArrayList<>();


    // Upload parts
    String content = generateRandomContent(5);
    int partNumber = 1;

    CompleteMultipartUploadRequest.Part
        part1 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part1);

    content = generateRandomContent(5);
    partNumber = 2;
    CompleteMultipartUploadRequest.Part
        part2 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part2);

    content = generateRandomContent(1);
    partNumber = 3;
    CompleteMultipartUploadRequest.Part
        part3 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part3);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);


    completeMultipartUpload(OzoneConsts.KEY, completeMultipartUploadRequest,
        uploadID);

    getObjectMultipart(OzoneConsts.KEY, 1);
  }

  private static String generateRandomContent(int sizeInMB) {
    int bytesToGenerate = sizeInMB * 1024 * 1024; // Convert MB to bytes
    byte[] randomBytes = new byte[bytesToGenerate];
    new SecureRandom().nextBytes(randomBytes);

    // Encode the random bytes to a Base64 string
    return Base64.getEncoder().encodeToString(randomBytes);
  }
}
