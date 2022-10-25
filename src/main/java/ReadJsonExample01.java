import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ReadJsonExample01 {

    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline        p = Pipeline.create(options);

        Schema beam_schema = new BeamShemaUtil(options.getSchemaFile()).convertAvroBeamSchema();

        PCollection<String> pc1 = p.apply(TextIO.read().from(options.getInputFile()).withDelimiter(new byte[] {'\n'}) ) ;
       // pc1.apply(MapElements.via(new ReadCsvExample01.StringPColPrinter() ));

        PCollection<Row>   pc2 = pc1.apply(JsonToRow.withSchema(beam_schema));
        pc2.apply(SqlTransform.query("select firstName, address from PCOLLECTION")).apply(ParDo.of(new ReadCsvExample01.RowPrinter()));

        PipelineResult result = p.run();
        result.waitUntilFinish();

    }

}
