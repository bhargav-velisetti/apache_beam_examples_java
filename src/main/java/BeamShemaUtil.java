import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import java.io.File;
import java.io.IOException;

public class BeamShemaUtil {
    String path;
   public BeamShemaUtil(String path) {
       this.path = path ;
   }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }


    public org.apache.avro.Schema getAvroSchema() throws IOException {

        org.apache.avro.Schema avro_schema =  new Schema.Parser().parse(new File(this.path));
        return avro_schema;

    }

    public org.apache.beam.sdk.schemas.Schema convertAvroBeamSchema() {

        try {
            return AvroUtils.toBeamSchema(getAvroSchema());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
   }




}
