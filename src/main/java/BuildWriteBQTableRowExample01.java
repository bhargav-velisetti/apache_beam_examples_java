import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import java.io.Serializable;


public class BuildWriteBQTableRowExample01 {


    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        options.setTempLocation("gs://temp_loc/temp/");

        TableReference tableReference = new TableReference();
        tableReference.setProjectId("project");
        tableReference.setDatasetId("dataset");
        tableReference.setTableId("table");


        Pipeline p = Pipeline.create(options);

        BeamShemaUtil beamShemaUtil = new BeamShemaUtil("data/sample-data-schema2.avsc");
        List<TableFieldSchema> slist = beamShemaUtil.convertBQTableSchema().getFields();


        PCollection<String> pc1 = p.apply(TextIO.read().from("data/sample-data.json").withDelimiter(new byte[] {'\n'}) ) ;

        PCollection<TableRow> pc2 = pc1.apply(MapElements.via(new CSVtoBQTablerows()));

        pc2.apply(BigQueryIO.writeTableRows()
                .withSchema(beamShemaUtil.convertBQTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .to(tableReference)
        );


        pc2.apply(MapElements.via(new SimpleFunction<TableRow, String>() {

            @Override
            public String apply(TableRow row) {

                System.out.println(row.toString());
                return  row.toString();

            }
        }));

        //pc1.apply(MapElements.via(new ReadCsvExample01.StringPColPrinter()));


        PipelineResult result = p.run();
        result.waitUntilFinish();

    }

    public static class CSVtoBQTablerows extends SimpleFunction<String, TableRow> {

        @Override
        public TableRow apply(String json) {

            TableRow row;

            try {
                InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
                row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            } catch (IOException e) {
                throw  new RuntimeException(e);
            }
            return  row;

        }

    }



}
