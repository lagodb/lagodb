/*
 * Execute named Spark SQL scenario steps and emit their actual outcomes.
 * The pg_regress expected files, rather than this runner, are the test oracle.
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SparkScenarioRunner {
  private static final Pattern DIRECTIVE =
      Pattern.compile("^-- step: ([a-z0-9_-]+)$");

  private record Step(String name, String sql) {}

  private SparkScenarioRunner() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException(
          "usage: SparkScenarioRunner <scenario-script>");
    }

    SparkSession spark =
        SparkSession.builder().appName("lagodb-spark-scenario").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    try {
      run(spark, parse(Path.of(args[0])));
    } finally {
      spark.stop();
    }
  }

  private static void run(SparkSession spark, List<Step> steps) {
    for (Step step : steps) {
      System.out.println("spark: " + step.name());
      try {
        List<Row> rows = spark.sql(step.sql()).collectAsList();
        for (Row row : rows) {
          StringBuilder output = new StringBuilder("spark: ");
          for (int column = 0; column < row.size(); column++) {
            if (column > 0) {
              output.append('|');
            }
            output.append(row.isNullAt(column) ? "NULL" : row.get(column));
          }
          System.out.println(output);
        }
      } catch (Exception error) {
        System.out.println("spark: ERROR: " + diagnostic(error));
      }
    }
  }

  private static List<Step> parse(Path path) throws Exception {
    List<Step> steps = new ArrayList<>();
    String name = null;
    StringBuilder sql = new StringBuilder();
    for (String line : Files.readAllLines(path)) {
      Matcher directive = DIRECTIVE.matcher(line);
      if (directive.matches()) {
        if (name != null) {
          steps.add(new Step(name, sql(sql, path)));
        }
        name = directive.group(1);
        sql.setLength(0);
      } else if (name != null) {
        sql.append(line).append('\n');
      } else if (!line.isBlank()) {
        throw new IllegalArgumentException(
            "SQL appears before the first directive in " + path);
      }
    }
    if (name != null) {
      steps.add(new Step(name, sql(sql, path)));
    }
    if (steps.isEmpty()) {
      throw new IllegalArgumentException("no Spark SQL steps found in " + path);
    }
    return steps;
  }

  private static String sql(StringBuilder source, Path path) {
    String sql = source.toString().strip();
    if (sql.endsWith(";")) {
      sql = sql.substring(0, sql.length() - 1).stripTrailing();
    }
    if (sql.isEmpty()) {
      throw new IllegalArgumentException("empty Spark SQL step in " + path);
    }
    return sql;
  }

  private static String diagnostic(Throwable error) {
    Throwable root = error;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    String message = root.getMessage();
    if (message == null || message.isBlank()) {
      return root.getClass().getName();
    }
    return message
        .strip()
        .replace("\\", "\\\\")
        .replace("\r", "\\r")
        .replace("\n", "\\n");
  }
}
