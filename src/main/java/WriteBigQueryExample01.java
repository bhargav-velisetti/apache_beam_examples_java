
import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class WriteBigQueryExample01 {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setTempLocation("data/temp/");
        options.setRunner(DirectRunner.class);
        Pipeline p              = Pipeline.create(options);

        BeamShemaUtil beamShemaUtil = new BeamShemaUtil("data/ship_data_schema.avsc");
        org.apache.avro.Schema avro_schema = beamShemaUtil.getAvroSchema();
        org.apache.beam.sdk.schemas.Schema beam_schema = beamShemaUtil.convertAvroBeamSchema();
        ValueProvider<org.apache.beam.sdk.schemas.Schema> schema =  ValueProvider.StaticValueProvider.of(beam_schema);

        PCollection<String> pc1 = p.apply(TextIO.read().from("ship_data.csv"));
        PCollection<Row> pc2 =pc1.apply(ParDo.of(new ReadCsvExample01.CSVRowConverter(schema))).setRowSchema(beam_schema) ;

        TableReference tableReference = new TableReference();
        tableReference.setProjectId("PROJECT");
        tableReference.setDatasetId("dataset");
        tableReference.setTableId("bq_table");


        pc2.apply(BigQueryIO.<Row>write()
                .to("PROJECT:dataset.bqtable") // or tableReference
                .withSchema(BigQueryUtils.toTableSchema(beam_schema))
               //.useBeamSchema(beam_schema) // either useBeamSchema or withSchema
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        );

        PipelineResult result = p.run();
        result.waitUntilFinish();

    }

}
