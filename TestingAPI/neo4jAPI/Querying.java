package neo4jAPI;

import org.neo4j.cypher.internal.expressions.In;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.parboiled.common.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Querying {
    public static int threshold = 700000;
    public static void main(String[] args){
        String pathName = "";
        String dataset = "";
        String dbName = "";
        File dataBaseFile = new File(pathName);
        DatabaseManagementServiceBuilder dmsb = new DatabaseManagementServiceBuilder(dataBaseFile);
        DatabaseManagementService dms = dmsb.build();
        GraphDatabaseService db = dms.database(dbName);
        try (Transaction tx = db.beginTx()) {
            File file = new File(dataset);
            InputStream in = new FileInputStream(file);
            byte[] temp = new byte[26];
            Set<Tuple2<Integer, Integer>> set = new HashSet<>();
            //int threshold = 1000000;
            int cont = 0;
            while(in.read(temp) != -1){
                if(++cont == threshold){
                    break;
                }
                byte[] startByte = new byte[4];
                System.arraycopy(temp, 0, startByte, 0, 4);
                byte[] endByte = new byte[4];
                System.arraycopy(temp, 4, endByte, 0, 4);
                int startPoint = LHtoInt(startByte);
                int endPoint = LHtoInt(endByte);
                set.add(new Tuple2<>(startPoint, endPoint));
            }
            long time_start = System.currentTimeMillis();
            int cnt = 0;
            for(Tuple2<Integer, Integer> s : set){
                //System.out.println("startP " + s.a);
                //System.out.println("endP " + s.b);
                int a = s.a;
                int b = s.b;
                Iterator<Relationship> it = tx.getRelationships(a, b);
                while(it.hasNext()){
                    //Relationship relationship = it.next();
                    Relationship relationship = it.next();
                    Integer sp = (Integer) relationship.getProperties("StartPoint").get("StartPoint");
                    Integer ep = (Integer) relationship.getProperties("EndPoint").get("EndPoint");
                    if(sp.equals(s.a) && ep.equals(s.b))
                        cnt++;
                }
            }
            long time_end = System.currentTimeMillis();
            System.out.println("WCSketch:");
            System.out.println("cnt: " + cnt);
            System.out.println("Takes time:");
            System.out.println(time_end - time_start);
            System.out.println();
            tx.commit();
            in.close();
        } catch(Exception e){
            e.printStackTrace();
        }

        try (Transaction tx = db.beginTx()) {
            System.out.println("in the tx");
            File file2 = new File(dataset);
            InputStream in2 = new FileInputStream(file2);
            byte[] temp = new byte[26];
            Set<Tuple2<Integer, Integer>> set = new HashSet<>();
            //int threshold = 1000000;
            int cont = 0;
            while(in2.read(temp) != -1){
                if(++cont == threshold){
                    break;
                }
                byte[] startByte = new byte[4];
                System.arraycopy(temp, 0, startByte, 0, 4);
                byte[] endByte = new byte[4];
                System.arraycopy(temp, 4, endByte, 0, 4);
                int startPoint = LHtoInt(startByte);
                int endPoint = LHtoInt(endByte);
                set.add(new Tuple2<>(startPoint, endPoint));
            }
            long time_start = System.currentTimeMillis();
            int cnt = 0;
            for(Tuple2<Integer, Integer> s : set){
                Node node = tx.findNode(MyLabels.PERSON, "NodeID", s.a);
                Iterable<Relationship> re = node.getRelationships(MyRelationshipTypes.HAVE_DEALT_WITH);
                for(Relationship relationship : re){
                    Integer sp = (Integer) relationship.getProperties("StartPoint").get("StartPoint");
                    Integer ep = (Integer) relationship.getProperties("EndPoint").get("EndPoint");
                    if(sp.equals(s.a) && ep.equals(s.b))
                        cnt++;
                }
            }
            long time_end = System.currentTimeMillis();
            System.out.println("Neo4j local:");
            System.out.println("cnt: " + cnt);
            System.out.println("Takes time:");
            System.out.println(time_end - time_start);
            tx.commit();
            in2.close();
            System.out.println("transaction success");
        } catch(Exception e){
            e.printStackTrace();
        }


    }

    public static int LHtoInt(byte[] b){
        int res = 0;
        for(int i = 0; i < b.length; ++i){
            res += (b[i] & 0xff) << (8 * i);
        }
        return res;
    }

    enum MyLabels implements Label {
        STUDENTS, GAYS, MOVIES, PERSON
    }

    enum MyRelationshipTypes implements RelationshipType{
        IS_GAYED_WITH, PROPOSE, AM, HAVE_DEALT_WITH
    }
}
