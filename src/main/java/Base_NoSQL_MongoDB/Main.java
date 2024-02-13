package Base_NoSQL_MongoDB;

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;

public class Main {
    // Declaración variable estáticas para el scanner
    public static  Scanner sc = new Scanner(System.in);

    public static void main(String[] args) {
        //Creamos el objeto escanner para poder pedir datos al usuario por teclado
        Scanner sc = new Scanner(System.in);
        // 1. Declaromos una variable opcion y le asignamos valor 0
        int opcion=0;

        // Cremos un Bucle DO-WHILE para:
        // - Para que nada mas ejecutar el programa al usuario le salga directamente el menu de ejecuacion
        //   con las diferentes Consultas que aparecen, dentro del bucle tambien hay un Switch donde
        //   se explicara su función a continuation.... " No seas impaciente :) "

        do {
            System.out.println("0. Salir del Programa");
            System.out.println("1. Insertar Nueva Fecha en la base de datos MongoDB");
            System.out.println("2. Visualizar documentos ya existentes en la base de datos");
            System.out.println("3. Volcar datos a un fichero");
            System.out.println("4  Buscar resgistros con find()");
            System.out.println("5. Contabilizar el número de registros ");
            System.out.println("6. Borrar un registro");
            System.out.println("7. Actualización de registros");

            // preguntamos al usuario que opcion del menu quiere ejecutar
            System.out.println("introduzca una opcion: ");
            opcion =sc.nextInt(); // y gracias a la variable creada anteriormente se podra almacenar lo que introduzca el usario

            // Swith sirve para ejecutar cada una de las opciones del Menu, y ejecutará un case u otro depndiendo de la opcion elegida por el usuario
            // que gracias a la variable  opcion y el objeto scanner podemos almacenar el valor ingresado por usuario y ahi el progrma ejecutara un case o otro.
            // Cada case del Switch tiene un metodo donde ejecutara cada consulta que aparecera en el menu, (él case 0 es para salir de progrma)

            switch (opcion) {
                case 0:
                    // Directamente, te muestra un mensaje dandote las gracias por usar el programa
                    // ahi el programa ya finaliza su Ejecucion
                    System.out.println("gracias por usar el prgrama");
                    break;
                case 1:
                    insertarDocumentos();

                    break;
                case 2:
                    visualizarColeccion();

                    break;
                case 3:
                    MongoDBToTextFile();

                    break;
                case 4:
                    busqueda_registros();

                    break;
                case 5:
                    contabilizar_numero_registros();

                    break;
                case 6:
                    borrar_registro();

                    break;
                case 7:


                    break;

            }

        }while(opcion != 0); // Este bucle comenzará ejecutándose siempre que la condición entre paréntesis ( opcion != 0) sea verdadera.



    }

    private static void borrar_registro() {
        Scanner sc = new Scanner(System.in);
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            System.out.println("Conexión exitosa a MongoDB.");

            String introducir_nombre_BD = "";
            String introducir_nombre_coleccion = "";

            System.out.println("¿Introduce el nombre de la base de datos?");
            introducir_nombre_BD = sc.next();
            System.out.println("¿Introduce el nombre de la Colección?");
            introducir_nombre_coleccion = sc.next();

            MongoDatabase database = mongoClient.getDatabase(introducir_nombre_BD);
            MongoCollection<Document> collection = database.getCollection(introducir_nombre_coleccion);

            System.out.println("Introduce el campo para borrar registros:");
            String campo = sc.next();

            System.out.println("Introduce el valor para borrar registros:");
            String valor = sc.next();

            // Crear un filtro basado en el campo y el valor proporcionados
            Document filterDocument = new Document(campo, valor);

            // Borrar todos los documentos que coincidan con el filtro
            DeleteResult result = collection.deleteMany(filterDocument);

            if (result.getDeletedCount() > 0) {
                System.out.println("Registros borrados exitosamente.");
            } else {
                System.out.println("No se encontraron registros que coincidan con el filtro proporcionado.");
            }

        } catch (Exception e) {
            System.err.println("Error al conectar a MongoDB: " + e.getMessage());
        }
    }

    private static void contabilizar_numero_registros() {
        Scanner sc = new Scanner(System.in);
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            System.out.println("Conexión exitosa a MongoDB.");

            //String introducir_nombre_BD = "";
            //String introducir_nombre_coleccion = "";

            //System.out.println("¿introduce el nombre de la base de datos?");
            //introducir_nombre_BD = sc.next();
            //System.out.println("¿ el nombre de la Coleccion?");
            //introducir_nombre_coleccion = sc.next();

            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            System.out.println("Conteo de documentos en la colección:");

            long count = collection.countDocuments();
            System.out.println("Número de registros: " + count);

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
