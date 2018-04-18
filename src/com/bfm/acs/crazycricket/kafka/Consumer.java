package com.bfm.acs.crazycricket.kafka;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.bfm.acs.crazycricket.CrazyCricketProtos.Game;
import com.bfm.acs.crazycricket.CrazyCricketProtos.Game.GameType;
import com.bfm.acs.crazycricket.CrazyCricketProtos.Player;
import com.bfm.acs.crazycricket.constants.StringConstants;


public class Consumer {
	private static KafkaConsumer<String, byte[]> consumer;

	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, ParseException {
	     Properties props = new Properties();
	     try{
	    	 props.put("bootstrap.servers", args[0]);
	     }catch(Exception e){
	    	 System.out.println("No Kafka Broker provided");
	    	 System.exit(0);
	     }
	     
	     props.put("group.id", "test");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

	     consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("TEST","TWENTY_TWENTY","LIMITED_OVERS"));
	     
	     System.out.println("Listening to Kafka Broker at : " + props.getProperty("bootstrap.servers"));
	     
	     while (true) {
	         ConsumerRecords<String, byte[]> records = consumer.poll(1);
	         
	         
	         for (ConsumerRecord<String, byte[]> record : records){
	        	 Game game = Game.parseFrom(record.value());
	        	 Player winner = game.getWinner();
	        	 Player loser = game.getLoser();
	        	 GameType gameType = game.getType();
	        	 long gameDate = game.getGameDate();
	        	 
	        	 String winnerUserId = winner.getUserId();
	        	 String winnerCountry = winner.getCountry();
	        	 
	        	 String loserUserId = loser.getUserId();
	        	 String loserCountry = loser.getCountry();

	        	 JSONArray ja = new JSONArray();
	        	 
	        	 
	        	 File file = new File(StringConstants.pathDatasource);
	        	 
	        	 if(file.exists()){
	        		 FileReader fr = new FileReader(file);
	        		 JSONParser parser = new JSONParser();
	        		 ja = (JSONArray) parser.parse(fr);
	        		 fr.close();
	        	 }
	        	 
	        	 //System.out.println("Delete : " + f.delete());
	        	 
	        	 
	        	 	        	 
	        	 JSONObject jo = new JSONObject();
	        	 jo.put("winnerUserId", winnerUserId);
	        	 jo.put("winnerCountry", winnerCountry);
	        	 jo.put("loserUserId", loserUserId);
	        	 jo.put("loserCountry", loserCountry);
	        	 jo.put("gameType", gameType.toString());
	        	 jo.put("gameDate", gameDate);

	        	 ja.add(jo);
	        	 
	        	 FileWriter fw = new FileWriter(file);
	        	 try{
	        		 fw.write(ja.toString());
	        		 System.out.println("Successfully write JSON Objects to file");
	        	 }catch(Exception e){
	        		 e.printStackTrace();
	        	 }finally{
	        		 fw.flush();
	        		 fw.close();
	        	 }
	         }
	         
        	 
        	 

	     }
	}
}
