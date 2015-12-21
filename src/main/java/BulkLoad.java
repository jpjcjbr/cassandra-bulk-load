
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
public class BulkLoad {
	private static final String INPUT_FILE = "/Users/jpjcjbr/Downloads/201510_BolsaFamiliaFolhaPagamento.csv";

	/** Default output directory */
	public static final String DEFAULT_OUTPUT_DIR = "data";

	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	/** Keyspace name */
	public static final String KEYSPACE = "dados";

	/** Table name */
	public static final String TABLE = "bolsa_familia";

	/**
	 * Schema for bulk loading table. It is important not to forget adding
	 * keyspace name before table name, otherwise CQLSSTableWriter throws
	 * exception.
	 */
	public static final String SCHEMA = String.format(
			"CREATE TABLE IF NOT EXISTS %s.%s (UF text, CITY text, RECEIVER text, VALUE decimal, MONTH text, PRIMARY KEY ((MONTH, UF, CITY), RECEIVER))",
			KEYSPACE, TABLE);

	/**
	 * INSERT statement to bulk load. It is like prepared statement. You fill in
	 * place holder for each data.
	 */
	public static final String INSERT_STMT = String
			.format("INSERT INTO %s.%s (month, uf, city, receiver, value) VALUES (?, ?, ?, ?, ?)", KEYSPACE, TABLE);

	public static void main(String[] args) {
		Config.setClientMode(true);

		File outputDir = createOutputDirectory();

		CQLSSTableWriter writer = CQLSSTableWriter.builder()
				.inDirectory(outputDir)
				.forTable(SCHEMA)
				.using(INSERT_STMT)
				.withPartitioner(new Murmur3Partitioner())
				.build();

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(INPUT_FILE)));
				CsvListReader csvReader = new CsvListReader(reader, CsvPreference.TAB_PREFERENCE)) {
			
			csvReader.getHeader(true);

			List<String> line;
			while ((line = csvReader.read()) != null) {
				String month = line.get(11);
				String state = line.get(0);
				String city = line.get(2);
				String receiver = line.get(8);
				BigDecimal value = getValue(line);
				
				writer.addRow(month, state, city, receiver, value);
			}
			
		} catch (InvalidRequestException | IOException e) {
			e.printStackTrace();
		}

		try {
			writer.close();
		} catch (IOException ignore) {
		}
	}

	private static File createOutputDirectory() {
		File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			throw new RuntimeException("Cannot create output directory: " + outputDir);
		}
		return outputDir;
	}

	private static BigDecimal getValue(List<String> line) {
		try {
			return new BigDecimal(line.get(10));
		} catch (Exception e) {
			return BigDecimal.ZERO;
		}
	}
}
