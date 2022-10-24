import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    // --input=C:\Users\bharg\Documents\dataeng\data\data.csv --output=C:\Users\bharg\Documents\dataeng\output\test     CLI args passed like this

    public void setSchemaFile(String schemaFile);
    public String getSchemaFile();

    public void setInputFile(String inputFile);
    public String getInputFile();

    public void setOutputFile(String outputFile);
    public String getOutputFile();
}