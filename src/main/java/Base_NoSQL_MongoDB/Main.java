package Base_NoSQL_MongoDB;

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.result.UpdateResult;

public class Main {
    // Declaración variable estáticas para el scanner
    public static  Scanner sc = new Scanner(System.in);

    public static void main(String[] args) throws ParseException {
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
                    actualizar_registro();

                    break;

            }

        }while(opcion != 0); // Este bucle comenzará ejecutándose siempre que la condición entre paréntesis ( opcion != 0) sea verdadera.



    }

    private static void actualizar_registro() {
        Scanner sc = new Scanner(System.in);

        // Pedir al usuario el ID del registro a actualizar
        System.out.println("Introduzca el ID del registro que desea actualizar: ");
        String idStr = sc.nextLine();

        // Convertir la cadena de texto en un ObjectId
        ObjectId id = new ObjectId(idStr);

        // Crear el filtro para la consulta
        Bson filtro = new Document("_id", id);

        // Pedir al usuario la nueva fecha
        System.out.println("Introduzca la nueva fecha (en formato yyyy-MM-dd HH:mm:ss): ");
        String nuevaFechaStr = sc.nextLine();

        // Convertir la cadena de texto en un objeto Date
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // Ejemplo 2024-02-14 11:11:11
        Date nuevaFecha;
        try {
            nuevaFecha = dateFormat.parse(nuevaFechaStr);
        } catch (ParseException e) {
            System.out.println("Formato de fecha incorrecto. Asegúrese de utilizar el formato yyyy-MM-dd HH:mm:ss.");
            return;
        }

        // Crear el documento de actualización
        Bson update = new Document("$set", new Document("fecha", nuevaFecha));

        // Actualizar el registro utilizando updateOne()
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            UpdateResult result = collection.updateOne(filtro, update);

            if (result.getModifiedCount() == 1) {
                System.out.println("Registro actualizado exitosamente.");
            } else {
                System.out.println("No se encontró ningún registro con el ID especificado.");
            }
        } catch (Exception e) {
            System.err.println("Error al conectar a MongoDB: " + e.getMessage());
        }
    }

    private static void borrar_registro() {
        Scanner sc = new Scanner(System.in);

        // Pedir al usuario el ID del registro a eliminar
        System.out.println("Introduzca el ID del registro que desea eliminar: ");
        String idStr = sc.nextLine();

        // Convertir la cadena de texto en un ObjectId
        ObjectId id = new ObjectId(idStr);

        // Crear el filtro para la consulta
        BsonDocument filtro = new BsonDocument("_id", new BsonObjectId(id));

        // Eliminar el registro utilizando deleteOne()
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            DeleteResult result = collection.deleteOne(filtro);

            if (result.getDeletedCount() == 1) {
                System.out.println("Registro eliminado exitosamente.");
            } else {
                System.out.println("No se encontró ningún registro con el ID especificado.");
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

        // Pedir al usuario el ID del registro a buscar
        System.out.println("Introduzca el ID del registro que desea buscar: ");
        String idStr = sc.nextLine();

        // Convertir la cadena de texto en un ObjectId
        ObjectId id = new ObjectId(idStr);

        // Crear el filtro para la consulta
        Bson filtro = new Document("_id", id);

        // Realizar la búsqueda utilizando find()
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase database = mongoClient.getDatabase("miBaseDeDatos");
            MongoCollection<Document> collection = database.getCollection("micoleccion");

            FindIterable<Document> result = collection.find(filtro);

            if (result.iterator().hasNext()) {
                // Mostrar el resultado de la búsqueda
                for (Document document : result) {
                    System.out.println(document.toJson());
                }
            } else {
                System.out.println("No se encontró ningún registro con el ID especificado.");
            }
        } catch (Exception e) {
            System.err.println("Error al conectar a MongoDB: " + e.getMessage());
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
