package main.java;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.com.lmax.disruptor.TimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.util.retry.RetryBuilder;
import rx.Observable;
import rx.functions.Func1;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class DataLoadTool {

    //If the passed params are not as per specification this method prints help message
    private static void showHelpAndExit(){

        System.out.println("*****************************************");
        System.out.println("Format to run:  java DataLoadTool <<no_of_docs>> <<doc_size>>");
        System.out.println("    <no_of_docs> -> Number of JSON documents needed to populate in one batch");
        System.out.println("    <doc_size>> -> Document size in Bytes (this value can be any number but more than 15)");

        System.out.println("\n Eg:  If you wan to poulate 10 docs each of 20 Bytes\n" +
                           "\t java DataLoadTool 10 20");

        System.out.println("*****************************************\n");
        System.exit(0);
    }

    //This method appends extra bytes to JSON string value as needed
    private static String appendValueWithBytes(String value, int times){

        StringBuilder customValue = new StringBuilder(value);
        for(int i=0; i<times; ++i){
            customValue.append(new Random().nextInt(9));
        }
        return customValue.toString();
    }


    public static void main(String... args) {

        if(args.length<2){

            showHelpAndExit();
        }

        int docsToCreate=0, docSize=0;

        try{
            docsToCreate = Integer.parseInt(args[0]);
            docSize = Integer.parseInt(args[1]);

            if (docSize<16) showHelpAndExit();

            System.out.println(MessageFormat.format("Requested {0} documents each with size {1} Bytes",docsToCreate,docSize));


        }catch(NumberFormatException nfe){

            System.out.println("Arguments entered should be Integers");
            showHelpAndExit();
        }


        // Initializing connection
        Cluster cluster = CouchbaseCluster.create("localhost");
        cluster.authenticate("Administrator", "password");
        Bucket bucket = cluster.openBucket("default");



        // Generate sample 'n' number of JSON docs
        List<JsonDocument> documents = new ArrayList<>();

        while(docsToCreate > 0) {
            Long id = bucket.counter("id", 1,1).content();

            //Creating JSON value of length requested
            String value = appendValueWithBytes("tx", docSize-16);

            JsonObject content = JsonObject.create()
                    .put("n", 1)
                    .put("v", value);

            System.out.println(MessageFormat.format("Document created with size {0} Bytes",content.toString().getBytes().length ));

            documents.add(JsonDocument.create("doc-" + id, content));
            --docsToCreate;
        }



        // Insert in one batch using RxJava API
        Observable
                .from(documents)
                .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                    @Override
                    public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                        // Long id = bucket.counter("id", 1,1).content();
                        return bucket.async()
                                .insert(docToInsert)
                                .timeout(1000, TimeUnit.MILLISECONDS)
                                .retryWhen(RetryBuilder
                                        .anyOf(BackpressureException.class, TimeoutException.class)
                                        .delay(Delay.exponential(TimeUnit.MILLISECONDS, 250))
                                        .max(10)
                                        .build()
                                );
                    }
                })
                .last() //waiting until the last one is done
                .toBlocking()
                .single();

    }
}
