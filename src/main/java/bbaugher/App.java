package bbaugher;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.util.List;
import java.util.Map;

public class App {

    public static final RethinkDB r = RethinkDB.r;

    public static void main( String[] args ) {
        try {
            Connection conn = r.connection().hostname("localhost").port(28015).connect();

            List<String> tables = r.db("test").tableList().run(conn);
            if (!tables.contains("tv_shows")) {
                // Creates the table
                System.out.println("Creating table...");
                r.db("test").tableCreate("tv_shows").run(conn);
                System.out.println("Table created!");
                System.out.println();

                System.out.println("Inserting shows...");
                r.table("tv_shows").insert(r.array(
                        r.hashMap("name", "A Great Show!")
                                .with("rating", 10)
                                .with("actors", r.array(r.hashMap("name", "Bob"), r.hashMap("name", "Fred"))),
                        r.hashMap("name", "A Terrible Show")
                                .with("rating", 1)
                                .with("actors", r.array(r.hashMap("name", "Tom"), r.hashMap("name", "Jim"))),
                        r.hashMap("name", "Ehh")
                                .with("rating", 5)
                                .with("actors", r.array(r.hashMap("name", "Bob"), r.hashMap("name", "Alfred")))
                )).run(conn);
                System.out.println("Shows inserted!");
                System.out.println();
            }
            else {
                System.out.println("Table already exists");
                System.out.println();
            }

            // Retrieve all documents
            System.out.println("Fetching shows...");
            printDocs(r.table("tv_shows").run(conn));
            System.out.println("Fetched shows!");
            System.out.println();

            // Filtering by string matching
            System.out.println("Fetching shows with 'show' in name...");
            printDocs(r.table("tv_shows").filter(show -> show.g("name").match("Show")).run(conn));
            System.out.println("Fetched shows!");
            System.out.println();

            // Complex filter (nested filtering, chaining, numeric comparison)
            System.out.println("Fetching shows with actor who's name is Bob and rating > 5 ...");
            printDocs(r.table("tv_shows").filter(show -> show.g("actors").contains(actor -> actor.g("name").eq("Bob"))
                    .and(show.g("rating").gt(5))).run(conn));
            System.out.println("Fetched shows!");
            System.out.println();

            // Transforming documents into array of values from an array field in each document
            System.out.println("Fetching all distinct actors in any show...");
            List<String> actors = r.table("tv_shows").concatMap(show -> show.g("actors")).map(actor -> actor.g("name"))
                    .distinct().run(conn);
            System.out.println("Actors: " + actors);
            System.out.println("Fetched actors!");
            System.out.println();

            conn.close();
        } catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printDocs(Cursor cursor) {
        for (Object doc : cursor) {
            Map docMap = (Map) doc;
            System.out.println("Name: " + docMap.get("name"));
            System.out.println("Rating: " + docMap.get("rating"));

            for (Object actorDoc : ((List) docMap.get("actors"))) {
                Map actorDocMap = (Map) actorDoc;
                System.out.println("Actor:");
                System.out.println("  Name: " + actorDocMap.get("name"));
            }
        }
    }
}
