package x;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

public class SummarizationPOC {

	public static <T> void main(final String[] args) {
		final SparkConf conf = new SparkConf(true);
		conf.setMaster("local[4]");
		conf.setAppName("Teste");

		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		// conf.set("spark.cassandra.auth.username", "qa");
		// conf.set("spark.cassandra.auth.password", "qa");

		final JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<BolsaFamilia> mapped = CassandraJavaUtil.javaFunctions(sc).cassandraTable("dados", "bolsa_familia")
				.map(new Function<CassandraRow, BolsaFamilia>() {

					private static final long serialVersionUID = 1L;

					@Override
					public BolsaFamilia call(CassandraRow arg0) throws Exception {
						return new BolsaFamilia(arg0.getString("month"), arg0.getString("uf"), arg0.getString("city"),
								arg0.getString("receiver"), arg0.getDouble("value"), "123 - new - value");
					}
				});

		CassandraJavaUtil.javaFunctions(mapped)
			.writerBuilder("dados", "bolsa_familia", CassandraJavaUtil.mapToRow(BolsaFamilia.class)).saveToCassandra();;
		
		
		System.out.println("Result ========================= ");

	}
}
