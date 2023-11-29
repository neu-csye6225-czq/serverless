package com.neu.csye6225.czq;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.*;
import com.google.cloud.storage.*;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class Lambda implements RequestHandler<SNSEvent, String> {
    @Override
    public String handleRequest(SNSEvent event, Context context) {

        String submissionURL = event.getRecords().get(0).getSNS().getMessageAttributes().get("submission_url").getValue();
        String email = event.getRecords().get(0).getSNS().getMessageAttributes().get("email").getValue();
        String assignmentName = event.getRecords().get(0).getSNS().getMessageAttributes().get("assignment_name").getValue();

        String gcsBucketName = "your-gcs-bucket-name";
        String objectName = "release.zip";
        String tableName = "your-dynamodb-table-name";

        byte[] releaseContent;
        try {
            releaseContent = downloadFromGitHub(submissionURL);
        } catch (IOException e) {
            sendEmail(email, "Download failed", "email-domain");
            trackEmailSent(email, tableName, "failed");
            throw new RuntimeException(e);
        }
        sendEmail(email, "Download success", "email-domain");
        uploadToGCS(gcsBucketName, objectName, releaseContent);
        trackEmailSent(email, tableName, "success");

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

            return out.toByteArray();
        } finally {
            connection.disconnect();
        }
    }

    private static void uploadToGCS(String bucketName, String objectName, byte[] content) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        Blob blob = storage.create(BlobInfo.newBuilder(blobId).build(), content);
        System.out.println("File uploaded to GCS: " + blob.getName());
    }

    private static void sendEmail(String destEmail, String downloadStatus, String emailDomain) {
        AmazonSimpleEmailService client = AmazonSimpleEmailServiceClientBuilder.standard().build();

        // 创建发送邮件请求
        SendEmailRequest request = new SendEmailRequest()
                .withDestination(new Destination().withToAddresses(destEmail))
                .withMessage(new Message()
                        .withBody(new Body().withText(new Content().withCharset("UTF-8").withData(downloadStatus)))
                        .withSubject(new Content().withCharset("UTF-8").withData("Download Status")))
                .withSource(emailDomain);

        // 发送邮件
        SendEmailResult result = client.sendEmail(request);
        System.out.println("Email sent! Message ID: " + result.getMessageId());
    }

    private static void trackEmailSent(String userEmail, String tableName, String status) {
        // 在 DynamoDB 中记录发送的电子邮件
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

        Table table = dynamoDB.getTable(tableName);

        // 记录发送的电子邮件
        Item item = new Item()
                .withPrimaryKey("Email", userEmail)
                .withString("Status", status);

        table.putItem(item);
        System.out.println("Email sent status tracked in DynamoDB for: " + userEmail);
    }
}
