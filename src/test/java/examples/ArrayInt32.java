package examples;

import chdriver.core.Connection;
import chdriver.core.Decoder;
import chdriver.core.DriverProperties;
import chdriver.core.columns.Column;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.sql.*;
import java.util.Arrays;

import chdriver.core.Client;
import chdriver.core.ClickhouseProperties;
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
    private Decoder<Foo> scalaDecoder;

    @Before
    public void setUp() throws Exception {
        int http = chServer.getMappedPort(8123);
        int tcp = chServer.getMappedPort(9000);

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        conn = DriverManager.getConnection(DB_URL + http, USER, PASS);
        conn.setAutoCommit(false);
        stmt = conn.createStatement();

        createArrayInt32Table();
        populateArrayInt32Table();

        scalaClient = new Client(DriverProperties.DEFAULT_INSERT_BLOCK_SIZE(), new Connection("localhost", tcp, "q", "default", "default", ""));
        scalaClickhouseProperties = new ClickhouseProperties();
        scalaDecoder = new FooDecoder();
    }

    @Test
    public void testSelectArrayInt() throws Exception {
        int rowsNumber = 1000000;
        int times = 100;
        int[][] javaRes = new int[rowsNumber][];
        int[][] scalaRes = new int[rowsNumber][];
        String sql = "SELECT * FROM test_array_int32 limit " + rowsNumber;

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
            System.out.println("java = " + javaTime);

            now = System.currentTimeMillis();
            Iterator<Foo> it = scalaClient.execute(sql, scalaClickhouseProperties, scalaDecoder);
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

    class Foo {
        private int[] x;

        Foo(int[] x) {
            this.x = x;
        }
    }

    class FooDecoder implements Decoder<Foo> {

        @Override
        public boolean validate(String[] names, String[] types) {
            String[] expectedNames = {"x"};
            String[] expectedTypes = {"Array(Int32)"};
            return Arrays.equals(names, expectedNames) && Arrays.equals(types, expectedTypes);
        }

        @Override
        public Iterator<Foo> transpose(int numberOfItems, Column[] columns) {
            int[][] xs = new int[numberOfItems][];
            System.arraycopy(columns[0].data(), 0, xs, 0, numberOfItems);

            return new Iterator<Foo>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < numberOfItems;
                }

                @Override
                public Foo next() {
                    return new Foo(xs[i++]);
                }
            };
        }
    }

    private void createArrayInt32Table() throws Exception {
        String forCreate = "create table test_array_int32(x Array(Int32)) engine = Memory;";
        stmt.executeUpdate(forCreate);
    }

    private void populateArrayInt32Table() throws Exception {
        String forInsert = "insert into test_array_int32(x) values (?)";
        PreparedStatement ps = conn.prepareStatement(forInsert);
        for (int i = 0; i < 1_000_000; i++) {
            Integer[] ints = {1, 2, 3};
            ps.setArray(1, conn.createArrayOf("Int32", ints));
            ps.addBatch();
        }
        ps.executeBatch();
        conn.commit();
    }
}