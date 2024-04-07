import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;


public class ReadPubSubEample01 {

    public static void main(String[] args) {

        String mSubcription = "some-sub";
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setRunner(DirectRunner.class);
        options.setPubSubTopic(mSubcription);

        Pipeline p = Pipeline.create(options);

        PCollection<PubsubMessage> messages = p.apply("Read Messages From PubSub" , PubsubIO.readMessages().fromSubscription(options.getPubSubTopic()));

        messages.apply( "Print Messages Into Console" , MapElements.via( new MessagePrinter() ) );

        PCollection<String> messages2 =messages.apply( "PubSub Message to String Conversion",ParDo.of(new DoFn<PubsubMessage, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
               c.output( new String(c.element().getPayload(), StandardCharsets.UTF_8) ) ;
            }
        }));

        // Now using BQ Streaming Inserts. With setting up withMethod, we can use STORAGE API. Stoarge API is efficient and can handle huge data.
        PCollection<BigQueryInsertError> writeResult = messages.apply( "Write PubSub Messages to BQ" ,BigQueryIO.<PubsubMessage>write().to(record ->  writeToMultiTable(record))
                .withFormatFunction((PubsubMessage msg) -> convertJsonToTableRow(new String(msg.getPayload())))
                .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .useAvroLogicalTypes()
                .withoutValidation()
                .withExtendedErrorInfo()
              //  .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
              //  .withTriggeringFrequency(Duration.standardSeconds(2))
                ).getFailedInsertsWithErr();


        writeResult.apply("Printing Failed to Insert Into BQ", MapElements.via(new SimpleFunction<BigQueryInsertError, String>() {
            public String apply( BigQueryInsertError resultInsertError) {
                System.out.println( "Printing Failed to Insert Into BQ" + resultInsertError.getError());
                return resultInsertError.getRow().toString();
            }
        }));


        PipelineResult result =  p.run();
        result.waitUntilFinish(Duration.standardSeconds(10));

    }

    public static class MessagePrinter extends SimpleFunction<PubsubMessage, String> {
        public  String apply(PubsubMessage m){

            String minstring =  new String(m.getPayload(), StandardCharsets.UTF_8);
            System.out.println(m.getMessageId() + " ===> "  + minstring );
            return  m.toString();
        }
    }

    public static TableDestination writeToMultiTable( ValueInSingleWindow<PubsubMessage> record) {

        TableDestination dest;

        String project = "newdataeng";
        String dataset = "edw";

        if (record != null) {
            String minstring =  new String(record.getValue().getPayload(), StandardCharsets.UTF_8);
            Gson gson = new Gson();
            JsonObject jsonrecord = gson.fromJson(minstring, JsonObject.class);
            String t = jsonrecord.get("table_name").getAsString();
            String destTable = String.format("%s:%s.%s", project, dataset,t);
            dest = new TableDestination(destTable, null);
        } else {
            throw  new RuntimeException("no table in the message or message is null");
        }

        return dest;

    }

    public  static  TableRow convertJsonToTableRow( String msg){

        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + msg, e);
        }

        return row;

    }
}

