package com.neu.csye6225;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.mailgun.api.v3.MailgunMessagesApi;
import com.mailgun.client.MailgunClient;
import com.mailgun.model.message.Message;
import com.mailgun.model.message.MessageResponse;
import org.json.JSONObject;


import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Base64;
import java.nio.file.Files;

public class Lambda implements RequestHandler<SNSEvent, String> {

    public static void main(String[] args) throws IOException {
//        byte[] releaseContent = downloadFromGitHub("https://github.com/tparikh/myrepo/archive/refs/tags/v1.0.0.zip");
//        uploadToGCS("czq-csye6225-bucket", releaseContent);
//        System.out.println();
//        Files.write(Paths.get("/Users/cuizhiqing/Desktop/test.zip"), releaseContent);

//        uploadToGCS("czq-csye6225-bucket", releaseContent, credentials);
    }
    @Override
    public String handleRequest(SNSEvent event, Context context) {

        SNSRecord snsRecord = event.getRecords().get(0); // 获取第一个 SNS 记录

        String snsMessage = snsRecord.getSNS().getMessage(); // 获取 SNS 记录的消息

        System.out.println("Received JSON Message: " + snsMessage);

        JSONObject jsonObject = new JSONObject(snsMessage);
        String email = jsonObject.getString("email");
        String submissionURL = jsonObject.getString("submission_url");

        String googleAccessKey = System.getenv("GOOGLE_ACCESS_KEY");
        String gcsBucketName = System.getenv("BUCKET_NAME");
        String topicArn = System.getenv("SNS_TOPIC_ARN");
        String mailgunApikey = System.getenv("MAILGUN_APIKEY");
        String domain = System.getenv("DOMAIN");
        String dynamoDBTable = System.getenv("dynamoTable");

        System.out.println("domain " + domain);
        System.out.println("gcsBucketName " + gcsBucketName);
        System.out.println("dynamoDBTable " + dynamoDBTable);

        byte[] releaseContent;
        try {
            System.out.println("Downloading...");
            releaseContent = downloadFromGitHub(submissionURL);
            System.out.println("Uploading...");
            uploadToGCS(gcsBucketName, releaseContent);
        } catch (IOException e) {
            System.out.println("Sending failure email...");
            sendEmail(email, "Download failed", domain, mailgunApikey);

            System.out.println("Tracking failure email...");
            trackEmailSent(email, dynamoDBTable, "failed");
            throw new RuntimeException(e);
        }

        System.out.println("Sending success email...");
        sendEmail(email, "Download success!" + "Upload to " + gcsBucketName + " serverless", domain, mailgunApikey);

        System.out.println("Tracking success email...");
        trackEmailSent(email, dynamoDBTable, "success");

        return null;
    }


    private static byte[] downloadFromGitHub(String githubReleaseUrl) throws IOException {
        URL url = new URL(githubReleaseUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        try (BufferedInputStream in = new BufferedInputStream(connection.getInputStream());
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            System.out.println("Finish Downloading...");

            return out.toByteArray();
        } finally {
            connection.disconnect();
        }
    }

    private static void uploadToGCS(String bucketName, byte[] content) throws IOException {
        String credentialsJson = System.getenv("GOOGLE_ACCESS_KEY");
        byte[] decodedBytes = Base64.getDecoder().decode(credentialsJson);
        String decodedAccessKey = new String(decodedBytes);
        GoogleCredentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(decodedAccessKey.getBytes()));

        Storage storage = StorageOptions.newBuilder()
//                .setProjectId("czq-csye6225-dev-406620")
                .setProjectId("czq-csye6225-demo-406620")
                .setCredentials(credentials)
                .build().getService();

        BlobId blobId = BlobId.of(bucketName, "serverless");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.createFrom(blobInfo, new ByteArrayInputStream(content));
    }

    private static MessageResponse sendEmail(String destEmail, String downloadStatus, String emailDomain, String apiKey) {
        MailgunMessagesApi mailgunMessagesApi = MailgunClient.config(apiKey)
                .createApi(MailgunMessagesApi.class);

        Message message = Message.builder()
//                .from(emailDomain)
                .from("admin <csye6225@cuizhiqing.me>")
                .to(destEmail)
                .subject("CSYE6225-MSG")
                .text(downloadStatus)
                .build();

        return mailgunMessagesApi.sendMessage(emailDomain, message);
    }

    private static void trackEmailSent(String userEmail, String tableName, String status) {
        // 在 DynamoDB 中记录发送的电子邮件
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

        Table table = dynamoDB.getTable(tableName);

        // 记录发送的电子邮件
        Item item = new Item()
                .withPrimaryKey("ID", userEmail)
                .withString("S", status);

        table.putItem(item);
        System.out.println("Email sent status tracked in DynamoDB for: " + userEmail);
    }
}
