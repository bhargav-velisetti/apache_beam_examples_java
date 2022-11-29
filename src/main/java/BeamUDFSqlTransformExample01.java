import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BeamUDFSqlTransformExample01 {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setRunner(DirectRunner.class);
        options.setJobName("udf_exp");

        Pipeline p = Pipeline.create();

        Schema beam_schema = new BeamShemaUtil("data/ship_data_schema.avsc").convertAvroBeamSchema();
        ValueProvider<Schema> schema = ValueProvider.StaticValueProvider.of(beam_schema);


        PCollection<String> pc1 = p.apply(TextIO.read().from("data/ship_data.csv"));
        PCollection<Row>    pc2 = pc1.apply(ParDo.of(new ReadCsvExample01.CSVRowConverter(schema))).setRowSchema(beam_schema);

        pc2.apply(SqlTransform.query("select upper_udf(Ship_name) as ShipName from PCOLLECTION")
                    .registerUdf("upper_udf", new BeamUdf()))
                    .apply(ParDo.of(new ReadCsvExample01.RowPrinter()));

        PipelineResult result = p.run();
        result.waitUntilFinish();

    }

    public static class BeamUdf implements SerializableFunction<String, String> {
        String out;
        @Override
        public String apply(String input) {
            if (input.startsWith("a") || input.startsWith("A")) {
                out = input;
            }
            return out;
        }
    }
}
