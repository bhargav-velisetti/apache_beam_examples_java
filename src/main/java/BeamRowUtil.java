import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class BeamRowUtil {

    public static class BeamRowToBQRow extends DoFn<Row, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            Row beam_row = c.element();
            TableRow bq_row = BigQueryUtils.<Row>toTableRow(beam_row) ;
            //System.out.println(bq_row.toString());
            c.output(bq_row);
        }
    }

    public static class TableRowPrinter extends  DoFn<TableRow, Void> {
        @ProcessElement
        public  void processElement(ProcessContext c) {
            System.out.println(c.element().toString());
        }
    }


}
