
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{
//    private static final String INPUT_FILE = "/Users/jpjcjbr/Downloads/201510_BolsaFamiliaFolhaPagamento.csv";
    private static final String INPUT_FILE = "src/main/resources/teste.csv";

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /** Keyspace name */
    public static final String KEYSPACE = "dados";
    /** Table name */
    public static final String TABLE = "bolsa_familia";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE IF NOT EXISTS %s.%s (UF text, CITY text, RECEIVER text, VALUE decimal, MONTH text, PRIMARY KEY ((MONTH, UF, CITY), RECEIVER))", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    
    public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (month, uf, city, receiver, value) VALUES (?, ?, ?, ?, ?)", KEYSPACE, TABLE);

    public static void main(String[] args)
    {
        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
               // set target schema
               .forTable(SCHEMA)
               // set CQL statement to put data
               .using(INSERT_STMT)
               // set partitioner if needed
               // default is Murmur3Partitioner so set if you use different one.
               .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(INPUT_FILE)));
				CsvListReader csvReader = new CsvListReader(reader, CsvPreference.TAB_PREFERENCE)) {
			csvReader.getHeader(true);

			// Write to SSTable while reading data
			List<String> line;
			while ((line = csvReader.read()) != null) {
				// We use Java types here based on
                    // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
                    writer.addRow(
                    		line.get(11),
                    		line.get(0),
                    		line.get(2),
                    		line.get(8),
                    		getValue(line)
                                  );
                }
            }
            catch (InvalidRequestException | IOException e)
            {
                e.printStackTrace();
            }

        try
        {
            writer.close();
        }
        catch (IOException ignore) {}
    }

	private static BigDecimal getValue(List<String> line) {
		try {
			return new BigDecimal(line.get(10));
		} catch(Exception e) {
			return BigDecimal.ZERO;
		}
	}
}
