package com.bfm.acs.crazycricket.restapi;

import static spark.Spark.get;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.bfm.acs.crazycricket.constants.StringConstants;
import com.bfm.acs.crazycricket.util.Validate;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;
 
public class Main {
	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		Spark.port(StringConstants.PORT);
		System.out.println("Listening on Port : " + StringConstants.PORT);
    	/**
    	 * This function handles requests for following URLs: 
    	 * 		1) http://<host>:<port>/api/leaderboard
    	 * 		2) http://<host>:<port>/api/leaderboard?<optional_parameters>
    	 * 
    	 * This function returns the number of matches won and lost by each country in JSON format.
    	 * Output format:
    	 * {
    	 * 		"country1":{
    	 * 			"win":<number of matches won>,
    	 * 			"lose":<number of matches lost>
    	 * 		},
    	 *     	"country2":{
    	 * 			"win":<number of matches won>,
    	 * 			"lose":<number of matches lost>
    	 * 		}
    	 * }
    	 * 
    	 * Optionally, parameters can be used to filter results for a particular date range.
    	 * Parameters:
    	 * 		1) start=MMddyyyy: Used to give start date
    	 * 		2) end=MMddyyyy: Used to give end date
    	 * 
    	 */
        get("/api/leaderboard",new Route(){
			@SuppressWarnings("unchecked")
			@Override
			public Object handle(Request request, Response response) throws Exception {
				request.port();
				JSONParser parser = new JSONParser();
				JSONArray ja = (JSONArray) parser.parse(new FileReader(StringConstants.pathDatasource));
				Iterator<JSONObject> iter = ja.iterator();
				JSONObject returnJSON = new JSONObject();
				
				if(request.queryParams().contains("start")&&request.queryParams().contains("end")){
					Date startDate = Validate.dateTime(request.queryParams("start"));
					Date endDate = Validate.dateTime(request.queryParams("end"));
					if(startDate!=null && endDate!=null){
						while(iter.hasNext()){
							JSONObject jo = iter.next();
							String winner = (String) jo.get("winnerCountry");
							String loser = (String) jo.get("loserCountry");

							Date gameDate = new Date((long) jo.get("gameDate"));
							if((gameDate.after(startDate)||gameDate.equals(startDate)) && (gameDate.before(endDate)||gameDate.equals(endDate))){
								incrementCountInJSON(returnJSON,winner,"win");
								incrementCountInJSON(returnJSON,loser,"lose");
							}
						}
					}
					else{
						returnJSON.put("Status", "Wrong Parameters");
					}

				}
				else{
					while(iter.hasNext()){
						JSONObject jo = iter.next();
						String winner = (String) jo.get("winnerCountry");
						String loser = (String) jo.get("loserCountry");
						
						incrementCountInJSON(returnJSON,winner,"win");
						incrementCountInJSON(returnJSON,loser,"lose");
					}
				}
				return returnJSON;
			}
        	
			
        });
        
    	/**
    	 * This function handles requests for following URLs: 
    	 * 		1) http://<host>:<port>/api/national_leaderboard
    	 * 		2) http://<host>:<port>/api/national_leaderboard?<optional_parameters>
    	 * 
    	 * This function returns the number of matches won and lost by each player of a country in JSON format.
    	 * Output format:
    	 * {
    	 * 		"player1":{
    	 * 			"win":<number of matches won>,
    	 * 			"lose":<number of matches lost>
    	 * 		},
    	 *     	"player2":{
    	 * 			"win":<number of matches won>,
    	 * 			"lose":<number of matches lost>
    	 * 		}
    	 * }
    	 * 
    	 * Optionally, parameters can be used to filter results for a particular date range.
    	 * Parameters:
    	 * 		1) country: This parameter is used to specify the country for which user wants to see the leaderboard. If not specified, it assumes values "India"
    	 * 		2) start=MMddyyyy: Used to give start date
    	 * 		3) end=MMddyyyy: Used to give end date
    	 * 
    	 */
        get("/api/national_leaderboard",new Route(){
        	@SuppressWarnings("unchecked")
        	@Override
        	public Object handle(Request request, Response response) throws Exception {
        		JSONParser parser = new JSONParser();
        		JSONArray ja = (JSONArray) parser.parse(new FileReader(StringConstants.pathDatasource));
        		Iterator<JSONObject> iter = ja.iterator();
        		JSONObject returnJSON = new JSONObject();
            	
        		String currentCountry = null;
        		if(request.queryParams("country")!=null){
        			currentCountry = request.queryParams("country");
        		}
        		else{
        			currentCountry = "India";
        		}
        		
        		if(request.queryParams().contains("start")&&request.queryParams().contains("end")){
        			Date startDate = Validate.dateTime(request.queryParams("start"));
        			Date endDate = Validate.dateTime(request.queryParams("end"));
        			if(startDate!=null && endDate!=null){
        				while(iter.hasNext()){
        					JSONObject jo = iter.next();
        					String winner = (String) jo.get("winnerUserId");
        					String loser = (String) jo.get("loserUserId");
        					String winnerCountry = (String) jo.get("winnerCountry");
        					String loserCountry = (String) jo.get("loserCountry");
        					
        					Date gameDate = new Date((long) jo.get("gameDate"));
        					if((gameDate.after(startDate)||gameDate.equals(startDate)) && (gameDate.before(endDate)||gameDate.equals(endDate))){
        						if(winnerCountry.equalsIgnoreCase(currentCountry)){
        							incrementCountInJSON(returnJSON,winner,"win");
        						}
        						if(loserCountry.equalsIgnoreCase(currentCountry)){
        							incrementCountInJSON(returnJSON, loser,"lose");
        						}
        					}
        				}
        			}
        			else{
        				returnJSON.put("Status", "Wrong Parameters");
        			}
        		}
            	else{
            		while(iter.hasNext()){
            			JSONObject jo = iter.next();
            			String winner = (String) jo.get("winnerUserId");
            			String loser = (String) jo.get("loserUserId");
            			String winnerCountry = (String) jo.get("winnerCountry");
            			String loserCountry = (String) jo.get("loserCountry");
            			
            			if(winnerCountry.equalsIgnoreCase(currentCountry)){
            				incrementCountInJSON(returnJSON,winner,"win");
            			}
            			if(loserCountry.equalsIgnoreCase(currentCountry)){
            				incrementCountInJSON(returnJSON, loser,"lose");
            			}
            		}
            	}
        		return returnJSON;
        	}
        });
        

        /**
         * This function handles requests for following URLs:
    	 * 		1) http://<host>:<port>/api/raw
    	 * 		2) http://<host>:<port>/api/raw?<optional_parameters>
         * 
         * This function returns the original JSON data stored in the data source.
         * This is used for debugging purpose.
         * 
         * Optionally, parameters can be used to filter results for a particular date range.
    	 * Parameters:
    	 * 		1) start=MMddyyyy: Used to give start date
    	 * 		2) end=MMddyyyy: Used to give end date
         */
        get("/api/raw",new Route() {
        	@SuppressWarnings("unchecked")
        	@Override
        	public Object handle(Request request, Response response) throws FileNotFoundException, IOException, ParseException {
        		JSONParser parser = new JSONParser();
        		JSONArray ja = (JSONArray) parser.parse(new FileReader(StringConstants.pathDatasource));
        		JSONArray returnJSON = new JSONArray();
        		Iterator<JSONObject> iter = ja.iterator();
            	
        		if(request.queryParams().contains("start")&&request.queryParams().contains("end")){
        			Date startDate = Validate.dateTime(request.queryParams("start"));
        			Date endDate = Validate.dateTime(request.queryParams("end"));
        			if(startDate!=null && endDate!=null){
        				while(iter.hasNext()){
        					JSONObject jo = iter.next();
        					Date gameDate = new Date((long) jo.get("gameDate"));
							
        					if((gameDate.after(startDate)||gameDate.equals(startDate)) && (gameDate.before(endDate)||gameDate.equals(endDate))){
        						returnJSON.add(jo);
        					}
        				}
        			}
        			else{
        				JSONObject jo = new JSONObject();
        				jo.put("Status", "Wrong Parameters");
        				returnJSON.add(jo);
        			}
        		}
        		else{
        			while(iter.hasNext()){
        				JSONObject jo = iter.next();
        				returnJSON.add(jo);
        			}
        		}
        		return returnJSON;
        	}
        });
	}
    
	@SuppressWarnings("unchecked")
	private static void incrementCountInJSON(JSONObject jsonObj,String country,String status){
		JSONObject innerObject = new JSONObject();
		if(jsonObj.containsKey(country)){
			innerObject = (JSONObject) jsonObj.get(country);
			if(innerObject.containsKey(status)){
				innerObject.put(status, (Integer)innerObject.get(status) + 1);
			}
			else{
				innerObject.put(status, 1);
			}
		}
		else{
			innerObject.put(status, 1);
		}
		jsonObj.put(country, innerObject);
	}
}