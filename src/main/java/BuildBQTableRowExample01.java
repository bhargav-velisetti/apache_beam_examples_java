import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;



public  class BuildBQTableRowExample01 {

    public static void main(String[] args) {
        PipelineOptions  opts = PipelineOptionsFactory.create();
        opts.setRunner(DirectRunner.class);
        Pipeline   p = Pipeline.create(opts);

        BeamShemaUtil beamShemaUtil = new BeamShemaUtil("data/ship_data_schema.avsc");
        org.apache.avro.Schema avro_schema = beamShemaUtil.getAvroSchema();
        org.apache.beam.sdk.schemas.Schema beam_schema = beamShemaUtil.convertAvroBeamSchema();
        TableSchema table_schema = org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSchema(beam_schema);

        PCollection<String> pc1 = p.apply(TextIO.read().from("data/ship_data.csv") );
        PCollection<Row>    pc2 =  pc1.apply(ParDo.of(new ReadCsvExample01.CSVRowConverter(ValueProvider.StaticValueProvider.of(beam_schema)))).setRowSchema(beam_schema) ;

       // pc2.apply(ParDo.of(new ReadCsvExample01.RowPrinter()));

        PCollection<TableRow>  pc3 = pc2.apply(ParDo.of(new BeamRowUtil.BeamRowToBQRow()));
        pc3.apply(ParDo.of(new BeamRowUtil.TableRowPrinter()));

        PipelineResult result = p.run();
        result.waitUntilFinish();

    }


}
