
package org.trp.awsappdev.lambda.worker;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

/**
 * Lambda function that reads messages from an SQS queue for counting the words on a given web page.
 */
public class WordCountMessageConsumer implements RequestHandler<Object, String> {
    private static final String QUEUE_NAME = "WordCountJobs";
    private static final String DB_TABLE_NAME = "word_count";
    private static final String DB_URL_COLUMN = "url";
    private static final String DB_SNIPPET_COLUMN = "snippet";

    @Override
    public String handleRequest(Object input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Starting...");
        AmazonSQSClient sqs = (AmazonSQSClient) new AmazonSQSClient().withRegion(Region.getRegion(Regions.US_WEST_2));
        try {
            int totalMessages = 0;
            String queue = null;
            // Get the queue
            GetQueueUrlRequest getQueueRequest = new GetQueueUrlRequest(QUEUE_NAME);
            GetQueueUrlResult getQueueResult = sqs.getQueueUrl(getQueueRequest);
            if ((getQueueResult != null) && (getQueueResult.getQueueUrl() != null)) {
                queue = getQueueResult.getQueueUrl();

                // Read the messages from the queue
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queue)
                    .withWaitTimeSeconds(Integer.valueOf(2))
                    .withAttributeNames(new String[] { "All" });
                List<Message> messages = sqs
                    .receiveMessage(receiveMessageRequest).getMessages();
                logger.log("Received " + messages.size() + " messages");
                // Process each message
                for (Message message : messages) {
                    totalMessages++;
                    try {
                        logger.log("Message body: " + message.getBody());

                        URL url = new URL(message.getBody());
                        String snippet = null;

                        AmazonDynamoDBClient dynamoDBClient = 
                            (AmazonDynamoDBClient) new AmazonDynamoDBClient().withRegion(Regions.US_WEST_2);
                        DynamoDB client = new DynamoDB(dynamoDBClient);

                        // Make sure the table exists
                        CreateTableRequest createTableRequest = 
                            new CreateTableRequest().withTableName(DB_TABLE_NAME)
                        .withKeySchema(new KeySchemaElement[] { new KeySchemaElement(DB_URL_COLUMN, KeyType.HASH),
                                                                new KeySchemaElement(DB_SNIPPET_COLUMN, KeyType.RANGE) })
                        .withAttributeDefinitions(new AttributeDefinition[] { new AttributeDefinition(DB_URL_COLUMN,
                                                                                                      ScalarAttributeType.S),
                                                                              new AttributeDefinition(DB_SNIPPET_COLUMN,
                                                                                                      ScalarAttributeType.S) })
                        .withProvisionedThroughput(new ProvisionedThroughput(Long.valueOf(10L), Long.valueOf(10L)));
                        
                        TableUtils.createTableIfNotExists(dynamoDBClient, createTableRequest);

                        Table table = client.getTable(DB_TABLE_NAME);

                        logger.log("Table " + table.getTableName() + " contains " + table.describe().getItemCount()
                                   + " items.");
                        if (table.describe().getItemCount().longValue() > 0L) {
                            // See if the URL is already in the DB
                            GetItemOutcome gio = table.getItemOutcome(DB_URL_COLUMN, url.toString());
                            if ((gio != null) 
                                && (gio.getGetItemResult() != null) 
                                && (gio.getGetItemResult().getItem() != null)) {
                                snippet = ((AttributeValue) gio.getGetItemResult().getItem().get(DB_SNIPPET_COLUMN)).getS();
                            }
                        }
                        if (snippet != null) {
                            logger.log("Entry for URL " + url.toString() + " is already in database");
                            continue;
                        }
                        logger.log("Count words on " + url.toString());

                        snippet = countWordsOnPage(url);

                        logger.log("Saving snippet in DB for URL: " + url + "\nSnippet: " + snippet);

                        Item item = new Item().withPrimaryKey(DB_URL_COLUMN, url.toString())
                                    .withString(DB_SNIPPET_COLUMN, snippet);
                        table.putItem(item);
                        logger.log(url.toString() + " saved to DB.");
                    }
                    catch (MalformedURLException mue) {
                        logger.log("Invalid URL provided in message - message will be skipped: " + mue.toString());
                    }
                    catch (ParseException pe) {
                        logger.log("Error parsing contents of page - message will be skipped: " + pe.toString());
                    }
                    catch (IOException e) {
                        logger.log("Error accessing page - message will be skipped: " + e.toString());
                    }
                    sqs.deleteMessage(queue, message.getReceiptHandle());
                }
                logger.log("Done - processed " + totalMessages + " messages.");
            }
            else {
                logger.log("No queue found for name WordCountJobs");
                logger.log("Listening for messages on queue " + queue + "...");
            }
        }
        catch (AmazonClientException ase) {
            logger.log(ase.toString());
        }
        return "done";
    }
    
    /**
     * This is not the most efficient approach, but it will work for the purposes of the exercise.  Ideally I'd stream
     * the contents of the page rather than loading it all into memory at once. 
     * @param url URL of the page on which to count the words.
     * @return HTML snippet containing a table of the top words on the referenced page.
     * @throws IOException 
     * @throws ParseException 
     */
    private String countWordsOnPage(URL url) throws ParseException, IOException {
        // Get the contents of the page
        String pageContents = getPageContents(url);
        
        Map<String, Integer> wordCounts = new HashMap<>();
        // Split on all non-alphabetic characters
        String[] words = pageContents.split("\\W");
        for (String word : words) {
            if (word == null || word.trim().isEmpty()) {
                continue;
            }
            if (wordCounts.get(word) == null) {
                wordCounts.put(word, 1);
            } else {
                wordCounts.put(word, wordCounts.get(word)+1);
            }
        }
        // Sort the map by value
        TreeSet<Entry<String, Integer>> sortedValues = new TreeSet<Entry<String,Integer>>(
            new Comparator<Entry<String,Integer>>() {
                @Override public int compare(Entry<String,Integer> e1, Entry<String,Integer> e2) {
                    return e1.getValue().compareTo(e2.getValue()) * -1;
                }
            });
        sortedValues.addAll(wordCounts.entrySet());
        // Build the HTML snippet
        StringBuffer snippet = 
            new StringBuffer("<table border=\"2\" cellpadding=\"5\">\n<tr><th>Word</th><th>Count</th></tr>");
        // Add the top 10 words
        int wordCount = 0;
        Iterator<Entry<String, Integer>> entries = sortedValues.iterator();
        while (entries.hasNext() && wordCount < 10) {
            Entry<String, Integer> entry = entries.next();
            snippet.append("<tr><td>")
            .append(String.valueOf(entry.getKey())).append("</td><td>").append(String.valueOf(entry.getValue()))
            .append("</td></tr>\n");
            wordCount++;
        }
        // Close the table
        snippet.append("</table>");
        return snippet.toString();
    }
    
    /**
     * Scrape the web page and return its contents as a single string.
     * @param url Page to scrape.
     * @return Page contents as a string.
     * @throws IOException 
     * @throws ParseException 
     */
    private String getPageContents(URL url) throws ParseException, IOException {
        PoolingHttpClientConnectionManager poolingManager = new PoolingHttpClientConnectionManager();
        poolingManager.setMaxTotal(5); 
        poolingManager.setDefaultMaxPerRoute(5);

        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(5000)
                .setConnectTimeout(1000)
                .setConnectionRequestTimeout(1000)
                .build();
        
        CloseableHttpClient httpClient = HttpClientBuilder.create()
                        .setDefaultRequestConfig(requestConfig)
                        .setConnectionManager(poolingManager)
                        .build();
        HttpRequestBase method = new HttpGet(url.toString());
        // Make the call
        try (CloseableHttpResponse response = httpClient.execute(method)) {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                return response.getStatusLine().toString();
            }
            return EntityUtils.toString(response.getEntity(), "UTF-8");
        }
    }
}
