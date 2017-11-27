package examples;

import chdriver.Connection;
import chdriver.Decoder;
import chdriver.DriverProperties;
import chdriver.columns.Column;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.sql.*;
import java.util.Arrays;

import chdriver.Client;
import chdriver.ClickhouseProperties;
import scala.collection.Iterator;

public class ArrayInt32 {
    private static final String DB_URL = "jdbc:clickhouse://localhost:";
    private static final String USER = "default";
    private static final String PASS = "";

    @Rule
    public GenericContainer chServer =
            new GenericContainer("yandex/clickhouse-server:latest")
                    .withExposedPorts(8123, 9000);
    private java.sql.Connection conn;
    private Statement stmt;
    private Client scalaClient;
    private ClickhouseProperties scalaClickhouseProperties;
    private Decoder<TestArray> scalaDecoder;

    @Before
    public void setUp() throws Exception {
        int http = chServer.getMappedPort(8123);
        int tcp = chServer.getMappedPort(9000);

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        conn = DriverManager.getConnection(DB_URL + http, USER, PASS);
        stmt = conn.createStatement();

        createArrayIntTable();
        populateArrayIntTable();

        scalaClient = new Client(DriverProperties.DEFAULT_INSERT_BLOCK_SIZE(), new Connection("localhost", tcp, "q", "default", "default", ""));
        scalaClickhouseProperties = new ClickhouseProperties();
        scalaDecoder = new TestArrayDecoder();
    }

    @Test
    public void testSelectArrayInt() throws Exception {
        int rowsNumber = 1000000;
        int times = 100;
        int[][] javaRes = new int[rowsNumber][];
        int[][] scalaRes = new int[rowsNumber][];
        String sql = "SELECT * FROM test_array limit " + rowsNumber;

        for (int i = 0; i < times; i++) {
            long javaTime = 0;
            long scalaTime = 0;
            long now = System.currentTimeMillis();
            int j = 0;

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                javaRes[j] = (int[]) rs.getArray("x").getArray();
                j++;
            }
            rs.close();
            javaTime += System.currentTimeMillis() - now;
            System.out.println("jdbc = " + javaTime);

            now = System.currentTimeMillis();
            scala.collection.Iterator<TestArray> it = scalaClient.execute(sql, scalaClickhouseProperties, scalaDecoder);
            j = 0;
            while (it.hasNext()) {
                scalaRes[j] = it.next().x;
                j++;
            }
            scalaTime += System.currentTimeMillis() - now;
            System.out.println("scala = " + scalaTime);

            assert Arrays.deepEquals(javaRes, scalaRes);
            System.out.println();
        }

        stmt.close();
        conn.close();
    }

    class TestArray {
        private int[] x;

        TestArray(int[] x) {
            this.x = x;
        }
    }

    class TestArrayDecoder implements Decoder<TestArray> {

        @Override
        public boolean validate(String[] names, String[] types) {
            String[] expectedNames = {"x"};
            String[] expectedTypes = {"Array(Int32)"};
            return Arrays.equals(names, expectedNames) && Arrays.equals(types, expectedTypes);
        }

        @Override
        public Iterator<TestArray> transpose(int numberOfItems, Column[] columns) {
            int[][] xs = new int[numberOfItems][];
            System.arraycopy(columns[0].data(), 0, xs, 0, numberOfItems);

            return new Iterator<TestArray>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < numberOfItems;
                }

                @Override
                public TestArray next() {
                    return new TestArray(xs[i++]);
                }
            };
        }
    }

    private void createArrayIntTable() throws Exception {
        String forCreate = "create table test_array(x Array(Int32)) engine = Memory;";
        stmt.executeUpdate(forCreate);
    }

    private void populateArrayIntTable() throws Exception {
        conn.setAutoCommit(false);
        String forInsert = "insert into test_array(x) values (?)";
        PreparedStatement ps = conn.prepareStatement(forInsert);
        for (int i = 0; i < 1_000_000; i++) {
            Integer[] ints = {1, 2, 3};
            ps.setArray(1, conn.createArrayOf("Int32", ints));
            ps.addBatch();
        }
        ps.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);
    }
}