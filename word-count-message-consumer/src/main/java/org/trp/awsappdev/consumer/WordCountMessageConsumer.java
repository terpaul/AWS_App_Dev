package org.trp.awsappdev.consumer;

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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class WordCountMessageConsumer {
    private static final String QUEUE_NAME = "WordCountJobs";
    private static final String DB_TABLE_NAME = "word_count";
    private static final String DB_URL_COLUMN = "url";
    private static final String DB_SNIPPET_COLUMN = "snippet";
    
    private final CloseableHttpClient httpClient;
    private boolean stop = false;
    private long sleepTime = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("Final version for Homework #4.");
        WordCountMessageConsumer consumer = new WordCountMessageConsumer();
        consumer.run();
    }
    
    public WordCountMessageConsumer() {
        PoolingHttpClientConnectionManager poolingManager = new PoolingHttpClientConnectionManager();
        poolingManager.setMaxTotal(5); 
        poolingManager.setDefaultMaxPerRoute(5);

        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(5000)
                .setConnectTimeout(1000)
                .setConnectionRequestTimeout(1000)
                .build();
        
        httpClient = HttpClientBuilder.create()
                        .setDefaultRequestConfig(requestConfig)
                        .setConnectionManager(poolingManager)
                        .build();
    }
    
    /**
     * Run the consumer.
     */
    private void run() {
        AmazonSQS sqs = new AmazonSQSClient();//FUCK AWS .withRegion(Region.getRegion(Regions.US_WEST_2));

        try {
            int totalMessages = 0;
            String queue = null;
            GetQueueUrlRequest getQueueRequest = new GetQueueUrlRequest(QUEUE_NAME);
            GetQueueUrlResult getQueueResult = sqs.getQueueUrl(getQueueRequest);
            if (getQueueResult != null && getQueueResult.getQueueUrl() != null) {
                queue = getQueueResult.getQueueUrl();
            } else {
                System.out.println("No queue found for name " + QUEUE_NAME);
                return;
            }
            System.out.println("Listening for messages on queue " + queue + "...");
            do {
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queue);
                receiveMessageRequest.setWaitTimeSeconds(2);
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withAttributeNames("All")).getMessages();
                System.out.println("Received " + messages.size() + " messages");
                for (Message message : messages) {
                    totalMessages++;
                    try {
                        processMessage(message);
                        if (stop) break;
                    }
                    catch (MalformedURLException mue) {
                        System.out.println("Invalid URL provided in message - message will be skipped: " + mue.toString());
                    }
                    catch (ParseException pe) {
                        System.out.println("Error parsing contents of page - message will be skipped: " + pe.toString());
                    }
                    catch (IOException e) {
                        System.out.println("Error accessing page - message will be skipped: " + e.toString());
                    }
                    sqs.deleteMessage(queue, message.getReceiptHandle());
                }
                Thread.sleep(sleepTime);
            } while (!stop);
            System.out.println("Done - processed " + totalMessages + " messages.");
        } catch (AmazonClientException ase) {
            ase.printStackTrace();
        }
        catch (InterruptedException ie) {
            System.out.println("Interrupted while sleeping...consumer will terminate.");
        }
        System.out.println("Exiting...");
    }
    
    private void processMessage(Message message) throws ParseException, IOException {
        System.out.println("Message body: " + message.getBody());
        
        // Extract the URL from the body
        URL url = new URL(message.getBody());

        if (inDatabase(url)) {
            System.out.println("Entry for URL " + url.toString() + " is already in database");
            return;
        }
        // Fetch the URL page
        System.out.println("Count words on " + url.toString());
        // Count words in the response entity if it's not null
        String snippet = countWordsOnPage(url);
        // Save in DynamoDB
        saveSnippet(url, snippet);
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
        HttpRequestBase method = new HttpGet(url.toString());
        // Make the call
        try (CloseableHttpResponse response = httpClient.execute(method)) {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                return response.getStatusLine().toString();
            }
            return EntityUtils.toString(response.getEntity(), "UTF-8");
        }
    }
    
    /**
     * Save the snippet in the database if it doesn't already exist.
     * @param url URL of the snippet
     * @param snippet Word count HTML snippet
     */
    private void saveSnippet(URL url, String snippet) {
        System.out.println("Saving snippet in DB...");
        System.out.println("URL: " + url + "\nSnippet: " + snippet);
        // For running locally
        
        HashMap<String, AttributeValue> key = new HashMap<>();
        key.put(DB_URL_COLUMN, new AttributeValue().withS(url.toString()));
        key.put(DB_SNIPPET_COLUMN, new AttributeValue().withS(snippet));
        
        DynamoDB client = getDynamoClient();
        Table table = client.getTable(DB_TABLE_NAME);
        Item item = new Item().withPrimaryKey(DB_URL_COLUMN, url.toString()).withString(DB_SNIPPET_COLUMN, snippet);
        table.putItem(item);
        System.out.println(url.toString() + " saved to DB.");
    }
    
    /**
     * Checks if an entry exists in the database for a given URL.
     * @param url URL to check.
     * @return <code>true</code> if an entry already exists.
     */
    private boolean inDatabase(URL url) {
        DynamoDB client = getDynamoClient();
        Table table = client.getTable(DB_TABLE_NAME);

        System.out.println("Table " + table.getTableName() + " contains " + table.describe().getItemCount() + " items.");

        if (table.describe().getItemCount() > 0) {
            GetItemOutcome gio = table.getItemOutcome(DB_URL_COLUMN, url.toString());
            if (gio != null && gio.getGetItemResult() != null && gio.getGetItemResult().getItem() != null) {
                return true;
            }
        }
        return false;
    }
    
    private DynamoDB getDynamoClient() {
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient().withRegion(Regions.US_WEST_2);

        // Create the table if it doesn't exist
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(DB_TABLE_NAME)
        .withKeySchema(new KeySchemaElement("url", KeyType.HASH), new KeySchemaElement("snippet", KeyType.RANGE))
        .withAttributeDefinitions(new AttributeDefinition("url", ScalarAttributeType.S), 
                                  new AttributeDefinition("snippet", ScalarAttributeType.S))
        .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L));
        TableUtils.createTableIfNotExists(dynamoDBClient, createTableRequest);
        

        return new DynamoDB(dynamoDBClient);
    }
    
    /**
     * Log message details for troubleshooting.
     * @param message Message to be logged.
     */
    @SuppressWarnings("unused")
    private void logMessage(Message message) {
        System.out.println("  Message Contents");
        System.out.println("    MessageId:     " + message.getMessageId());
        System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
        System.out.println("    Body:          " + message.getBody());
        for (Entry<String, String> entry : message.getAttributes().entrySet()) {
            System.out.println("  Attribute");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    Value: " + entry.getValue());
        }
    }
}