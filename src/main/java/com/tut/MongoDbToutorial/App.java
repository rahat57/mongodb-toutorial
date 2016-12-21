package com.tut.MongoDbToutorial;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import io.vertx.core.json.JsonObject;
import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.sun.xml.internal.ws.spi.db.DatabindingException;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {

		// Function to insert Document in MongoDb
		// insertDoc("tweets.txt","twitter", "tweets");

		// FUNCTION TO UPDATE DOCUMENT
		// updataDoc("twitter", "tweet");

		
	//	DBCursor cursor =
		
		// delete Doc
		//deleteDoc("twitter2", "tweets");
		
		searchDocIterable("twitter", "tweets", "cars");

		// Retrieve Doc from MongoDb
		// getDoc("twitter", "tweets");
		// searchDoc("twitter1", "tweets");
		
		
//		int i=1;
//		while (cursor.hasNext()) {
//			System.out.println("Inserted Document: " + i);
//			System.out.println(cursor.next());
//			i++;
//				}
		
		
	}
	
	// Function to query Document using iterable
		public static void searchDocIterable(String DbName, String ColName,String query) {

			// Connect to mongoDb Server
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			// Now connect to your databases
			MongoDatabase db = mongoClient.getDatabase(DbName);
			 FindIterable<Document> iterable = db.getCollection(ColName).find(new Document("$text", new Document("$search", query)));
			int i=1;
//			 if(iterable != null){
//				 for(Document doc: iterable){
//					 System.out.println(" NO of records :"+i +" "+doc.getString("text"));
//					 i++;
//				 }
//			 }
			// ColName).find(
			// (DBObject) new Document("$text", new Document("$search", query)));
//			 if (iterable == null) {
//			        System.out.println("Null : no data");
//			    }
//			 for(Document obj:iterable){
//				 System.out.println("Document data :"+obj.toJson());
//			 }
	
			 iterable.forEach(new Block<Document>()  {
				 JsonObject tweet;
				 int i=1;
				    public void apply(final Document document) {
				    	tweet = new JsonObject(document.getString("text"));
				    	//document.getString("text");
				        System.out.println(" NO of records :"+i +" "+document.getString("text"));
				        i++;
				    }
				});
			//System.out.println("does't contain anything :"+iterable.toString());
		}

		private class TgBlock implements Block<Document> {

			@Override
			public void apply(final Document document) {
				try{
					
					System.out.println(document.getString("tweet"));
				
				} catch ( DatabindingException e) {
					System.out.println("Error invoked here");
					e.printStackTrace();
				}
			}
		}
		
	// Function to query Document
	public static DBCursor searchDoc(String DbName, String ColName) {

		// Connect to mongoDb Server
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		// Now connect to your databases
		DB db = mongoClient.getDB(DbName);
		// FindIterable<Document> iterable = db.getCollection(
		// ColName).find(
		// (DBObject) new Document("$text", new Document("$search", query)));

		System.out.println("Connect to database successfully");
		DBCollection coll = db.getCollection(ColName);
		// iterable.forEach(new TgBlock());
		System.out.println("Collection " + ColName + " selected successfully");
		//
	
		BasicDBObject whereQuery = new BasicDBObject(DbName, ColName);
		whereQuery.put("_id.ObjectId", "57cd18276c04f14f18c4007e");
		// System.out.println("searched document :"+iterable.toString());

		DBCursor cursor = coll.find(whereQuery);
//		DBObject obj=(DBObject) coll.find();
//		
		int i=1;
		while (cursor.hasNext()) {
			System.out.println("Inserted Document: " + i);
			System.out.println(cursor.next());
			i++;
			//deleteDoc("twitter", "tweet");
		}
		//System.out.println("query :"+obj.get("text"));
		
//		while (cursor.hasNext()) {
//			System.out.println("Inserted Document: ");
//			System.out.println(cursor.next());
//
//		}
		return cursor;
	}

	// Function to query Document
	public static DBCursor deleteDoc(String DbName, String ColName) {

		DBCursor cursor = null;
		try {
			// Connect to mongoDb Server
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your databases
			DB db = mongoClient.getDB(DbName);
			System.out.println("Connect to database successfully");
			DBCollection coll = db.getCollection(ColName);
			System.out.println("Collection " + ColName
					+ " selected successfully");
			DBCursor obj=coll.find();
			//	DBObject myDoc = (DBObject) coll.find();
				coll.remove((DBObject) obj);
			//	cursor = coll.find();
				System.out.println("Document deleted successfully");
			
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return cursor;
	}

	// Function to update Document
	public static void updataDoc(String DbName, String ColName) {

		// Connect to mongoDb Server
		MongoClient mongoClient = new MongoClient("localhost", 27017);

		// Now connect to your databases
		DB db = mongoClient.getDB(DbName);
		System.out.println("Connect to database successfully");
		DBCollection coll = db.getCollection(ColName);
		System.out.println("Collection " + ColName + " selected successfully");

		BasicDBObject updateDocument = new BasicDBObject();
		updateDocument.append("$set",
				new BasicDBObject().append("index", "2ndlastIndex"));

		BasicDBObject searchQuery = new BasicDBObject().append("index", "1499");

		coll.update(searchQuery, updateDocument);

	}

	// Function to get Document
	public static DBCursor getDoc(String DbName, String ColName) {

		// Connect to mongoDb Server
		MongoClient mongoClient = new MongoClient("localhost", 27017);

		// Now connect to your databases
		DB db = mongoClient.getDB(DbName);
		System.out.println("Connect to database successfully");
		DBCollection coll = db.getCollection(ColName);
		System.out.println("Collection json selected successfully");
		DBCursor cursor = coll.find();
//		DBObject obj = coll.findOne();
//		System.out.println("text" + obj.get("text"));
		int i=1;
		while (cursor.hasNext()) {
			System.out.println("Inserted Document: " + i);
			System.out.println(cursor.next());
			i++;
			//deleteDoc("twitter", "tweet");
		}
		return cursor;
	}

	// Function to insert document into MongoDb
	public static void insertDoc(String fileName, String DbName, String ColName) {
		try {
			BufferedReader reader = loadFile(fileName);
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your databases
			DB db = mongoClient.getDB(DbName);
			System.out.println("Connect to database successfully");
			char arr[] = { 'r' };
			db.addUser("rahat", arr);

			DBCollection coll = db.getCollection(ColName);
			System.out.println("Collection json selected successfully");

			String line;
			BasicDBObject doc;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				
				doc = new BasicDBObject();
				doc.append("text", line);
				coll.insert(doc);
				System.out.println("Document inserted successfully");
				count++;
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

	}

	public static BufferedReader loadFile(String file)
			throws FileNotFoundException {

		// for reading files from root dorectory
		InputStream in = new FileInputStream(new File(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		return reader;
	}

	public static void writeFile(String Name, List<String> res) {
		FileWriter fw;
		try {
			fw = new FileWriter(new File("MongoTweetData.txt"));
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(res.toString());
			bw.close();
			System.out.println("SAVED !");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
