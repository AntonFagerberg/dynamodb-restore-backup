package dynamodb.backup.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class App {

    static AtomicLong grandTotal = new AtomicLong();
    static final long allStart = System.currentTimeMillis();

    public void run(Path file, String tableName, AmazonDynamoDB client) throws Exception {
        System.out.println(file.getFileName() + " start");

        final var dynamoDB = new DynamoDB(client);
        final var objectMapper = new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        final var start = System.currentTimeMillis();

        var totalInFile = 0L;

        try (FileInputStream fis = new FileInputStream(file.toString());
             GZIPInputStream gzis = new GZIPInputStream(fis);
             InputStreamReader reader = new InputStreamReader(gzis);
             BufferedReader in = new BufferedReader(reader)) {

            String line;
            final var maxBatchItems = 25;
            var items = new HashSet<Item>(maxBatchItems);

            while ((line = in.readLine()) != null) {
                if (items.size() == maxBatchItems) {
                    var outcome = dynamoDB.batchWriteItem(new TableWriteItems(tableName).withItemsToPut(items));

                    do {
                        if (outcome.getUnprocessedItems().size() != 0) {
                            outcome = dynamoDB.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
                        }
                    } while (outcome.getUnprocessedItems().size() > 0);

                    var currentGrandTotal = grandTotal.addAndGet(items.size());

                    if (currentGrandTotal % 100 == 0) {
                        System.out.println("Done " + currentGrandTotal + " items [" + elapsed(allStart) + "]");
                    }
                    totalInFile += items.size();
                    items.clear();
                }

                final var item = objectMapper.readTree(line).get("Item");
                final Map<String, AttributeValue> itemMap = objectMapper.convertValue(item, new TypeReference<>() {

                });

                items.add(ItemUtils.toItem(itemMap));
            }

            if (items.size() > 0) {
                var outcome = dynamoDB.batchWriteItem(new TableWriteItems(tableName).withItemsToPut(items));

                do {
                    if (outcome.getUnprocessedItems().size() != 0) {
                        outcome = dynamoDB.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
                    }
                } while (outcome.getUnprocessedItems().size() > 0);

                totalInFile += items.size();
            }
        }

        System.out.println(file.getFileName() + " done (" + totalInFile + " items) [" + elapsed(start) + "]");
    }

    static String elapsed(long start) {
        final var millis = System.currentTimeMillis() - start;
        return String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
                TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));
    }

    public static void main(String[] args) throws Exception {
        final var sourceFolder = "/folder/with/gzip/backup/files";
        final var tableName = "table_name";

        final var client = AmazonDynamoDBClientBuilder.standard()
                .withClientConfiguration(null)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("test", "test")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", "eu-west-1"))
                .build();

        final var files = Files.list(Path.of(sourceFolder)).collect(Collectors.toList());
// Uncomment to get some parallelization - but it doesn't make DynamoDB Local import any faster
//        final var availableProcessors = Runtime.getRuntime().availableProcessors();
//        System.out.println("Found " + availableProcessors + " available processors");
//        ExecutorService executor = Executors.newFixedThreadPool(availableProcessors);

        for (var file : files) {
//            executor.submit(() -> {
//                try {
                    new App().run(file, tableName, client);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
        }

//        executor.shutdown();
//        executor.awaitTermination(365, TimeUnit.DAYS);
        System.out.println("All done (" + grandTotal.get() + " items) [" + elapsed(allStart) + "]");
    }
}
