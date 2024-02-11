package Base_NoSQL_MongoDB;

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.client.*;
import org.bson.Document;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.println("Voy a visualizar lo insertado");
        insertarDocumentos();
        System.out.println("Voy a visualiizar lo insertado");
        visualizarColeccion();
        System.out.println("voy pasar los datos de un fichero");
        MongoDBToTextFile();
        System.out.println("voy a buscar un Registro");
        busqueda_registros();
        System.out.println("Voy a Contabilizar el número de registros de una búsqueda");
        contabilizar_numero_registros();

    }
    private static void contabilizar_numero_registros() {
        Scanner sc = new Scanner(System.in);
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            System.out.println("Conexión exitosa a MongoDB.");

           // String introducir_nombre_BD = "";
           // String introducir_nombre_coleccion = "";

           // System.out.println("¿Introduce el nombre de la base de datos?");
           // introducir_nombre_BD = sc.next();
           // System.out.println("¿El nombre de la Colección?");
           // introducir_nombre_coleccion = sc.next();

            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            System.out.println("Introduce los filtros de búsqueda en formato JSON (ejemplo: {campo: valor}):");
            String filtrosJson = sc.next();

            // Convierte la cadena JSON de filtros a un objeto Document de MongoDB
            Document filtros = Document.parse(filtrosJson);

            // Utiliza el método countDocuments para obtener el número de registros que coinciden con los filtros
            long count = collection.countDocuments(filtros);

            System.out.println("Número de registros que coinciden con los filtros: " + count);

        } catch (Exception e) {
            System.err.println("Error al conectar a MongoDB: " + e.getMessage());
        }
    }



    private static void busqueda_registros() {
        Scanner sc = new Scanner(System.in);
        try (MongoClient mongoClient =  MongoClients.create("mongodb://localhost:27017")){
            System.out.println("Conexión exitosa a MongoDB.");

            String introducir_nombre_BD="";
            String introducir_nombre_coleccion="";

            System.out.println("¿introduce el nombre de la base de datos?");
            introducir_nombre_BD= sc.next();
            System.out.println("¿ el nombre de la Coleccion?");
            introducir_nombre_coleccion= sc.next();


            MongoDatabase database = mongoClient.getDatabase(introducir_nombre_BD);
            MongoCollection<Document> collection = database.getCollection(introducir_nombre_coleccion);

            System.out.println("Documentos en la coleccion");

            FindIterable<Document> documents = collection.find();

            for (Document document: documents) {
                System.out.println(document.toJson());
            }

        }catch (Exception e){
            System.err.println("Error al conectar a mongoDB: " + e.getMessage());
        }

    }

    private static void MongoDBToTextFile() {
        try(var mongoclient = MongoClients.create("mongodb://localhost:27017")){
            var database = mongoclient.getDatabase("miBaseDeDatos");
            var collection = database.getCollection("micoleccion");

            try(MongoCursor<Document> cursor = collection.find().iterator()){
                String filePath = "datos_exportados.txt";

                try(FileWriter writer = new FileWriter(filePath)){
                    while(cursor.hasNext()){
                        Document document = cursor.next();
                        writer.write(document.toJson());
                        writer.write("\n");
                    }
                    System.out.println("datos exportados exitosamente a: "+filePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }
    }

    public static void visualizarColeccion(){

        try (MongoClient mongoClient =  MongoClients.create("mongodb://localhost:27017")){
            System.out.println("Conexión exitosa a MongoDB.");

            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            System.out.println("Documentos en la coleccion");

            FindIterable<Document> documents = collection.find();

            for (Document document: documents) {
                System.out.println(document.toJson());
            }

        }catch (Exception e){
            System.err.println("Error al conectar a mongoDB: " + e.getMessage());
        }

    }
    private static void insertarDocumentos() {
        try(MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")){
            System.out.println("conexion exitosa a MongoDB");

            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            Document document = new Document("fecha", new Date());
            collection.insertOne(document);

            System.out.println("Documento insertado en la coleccion");

        }catch (Exception e){
            System.err.println("Error al conectar a mongoDB: " + e.getMessage());
        }
    }
}
