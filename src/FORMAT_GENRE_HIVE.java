import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FORMAT_GENRE_HIVE extends UDF {
	public Text evaluate(Text input)

	{
		String str = input.toString();
		String[] genre = str.split("\\|");
		StringBuilder sb = new StringBuilder();

		//int count = 1;
		int length = genre.length;

		if (length == 1) {
			sb.append("1) " + genre[0] + " asv130130 :hive");
			return new Text(sb.toString());
		} else if (length == 2) {
			sb.append("1) " + genre[0] + " & 2) " + genre[1]
					+ " asv130130 :hive");
			return new Text(sb.toString());
		} else if (length == 3)
			sb.append("1)" + genre[0] + ", 2)" + genre[1] + " & 3)"
					+ genre[2] + " asv130130 :hive");

		return new Text(sb.toString());

	}
}