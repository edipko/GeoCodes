package com.spotonresponse.webservices;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


@WebServlet(name = "saberdata", value = "/saberdata")
public class GetSaberData extends HttpServlet {

    private static DynamoDB myDynamoDB;
    private static AmazonDynamoDB client;

    // Will be assigned from System.env()
    private static String DynamoDBTableName;


    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        String outputFormat = "raw";
        if (request.getParameter("outputFormat") != null) {
            outputFormat = request.getParameter("outputFormat");
        }

        // Get some envirnoment variables
        String amazon_endpoint = "https://dynamodb.us-east-1.amazonaws.com";
        String amazon_region = "us-east-1";
        String aws_access_key_id = "AKIAY3BUVZH2ESXEF6FQ";
        String aws_secret_access_key = "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";
        DynamoDBTableName = "SaberData";


        // Setup database connection
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(aws_access_key_id, aws_secret_access_key);

        client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(amazon_endpoint, amazon_region))
                .build();
        myDynamoDB = new DynamoDB(client);


        // Get all the keys and store them in an array
        Map<String, String> currentItems = new HashMap<String, String>();
        ScanResult scanResult = null;
        do {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(DynamoDBTableName);
            scanResult = client.scan(scanRequest);

            if (scanResult != null) {
                scanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
            }

            scanResult = client.scan(scanRequest);

            for (Map<String, AttributeValue> item : scanResult.getItems()) {
                currentItems.put(item.get("title").getS(), item.get("md5hash").getS());
            }
        } while (scanResult.getLastEvaluatedKey() != null);


        Table table = myDynamoDB.getTable(DynamoDBTableName);
        Iterator dbItemIterator = currentItems.entrySet().iterator();

        String output = "No Data";
        if (outputFormat.equals("geojson")) {
            output = outputJson(dbItemIterator, table);
        } else {

            output = outputRaw(dbItemIterator, table);
        }

        response.setContentType("application/json");
        response.getWriter().println(output);
    }


    private String outputRaw(Iterator dbItemIterator, Table table) {
        JSONArray geoJsonArray = new JSONArray();
        while (dbItemIterator.hasNext()) {
            Map.Entry dbPair = (Map.Entry) dbItemIterator.next();
            String dbKey = dbPair.getKey().toString();
            String dbIndex = dbPair.getValue().toString();

            GetItemSpec spec = new GetItemSpec()
                    .withPrimaryKey(new PrimaryKey("title", dbKey, "md5hash", dbIndex));
            Item item = table.getItem(spec);
            JSONObject properties = new JSONObject(item.toJSON());
            geoJsonArray.put(properties);
        }

        return new JSONArray(geoJsonArray.toString()).toString(3);
    }


    private String outputJson(Iterator dbItemIterator, Table table) {
        JSONArray featuresArray = new JSONArray();
        while (dbItemIterator.hasNext()) {
            Map.Entry dbPair = (Map.Entry) dbItemIterator.next();
            String dbKey = dbPair.getKey().toString();
            String dbIndex = dbPair.getValue().toString();

            GetItemSpec spec = new GetItemSpec()
                    .withPrimaryKey(new PrimaryKey("title", dbKey, "md5hash", dbIndex));
            Item item = table.getItem(spec);


            JSONObject properties = new JSONObject(item.toJSON());
            JSONObject itemJson = properties.getJSONObject("item");
            JSONObject where = itemJson.getJSONObject("where");
            JSONObject point = where.getJSONObject("Point");
            String pos = point.getString("pos");
            String[] loc = pos.split(" ");


            double latitude = Double.valueOf(loc[0]);
            double longitude = Double.valueOf(loc[1]);

            JSONArray coords = new JSONArray();
            coords.put(longitude);
            coords.put(latitude);

            JSONObject geometry = new JSONObject();
            geometry.put("type", "Point");
            geometry.put("coordinates", coords);

            JSONObject feature = new JSONObject();
            feature.put("type", "Feature");
            feature.put("geometry", geometry);
            feature.put("properties", itemJson);

            featuresArray.put(feature);

        }

        JSONObject fc = new JSONObject();
        fc.put("type", "FeatureCollection");
        fc.put("features", featuresArray);

        return new JSONObject(fc.toString()).toString(3);
    }

}
