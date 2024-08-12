
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  private Double correctServingRadius = 3.0;
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

      
      // long startTimeInMillis = System.currentTimeMillis();

// Call the function


      Double latitude = getRestaurantsRequest.getLatitude();
      Double longitude = getRestaurantsRequest.getLongitude();

      // LocalTime morningStartTime = LocalTime.of(7, 59);
      // LocalTime morningEndTime = LocalTime.of(10, 01);
      // LocalTime afternoonStartTime = LocalTime.of(12, 59);
      // LocalTime agternoonEndTime = LocalTime.of(14, 01);
      // LocalTime eveningStartTime = LocalTime.of(18, 59);
      // LocalTime eveningEndTime = LocalTime.of(21, 01);
      
      List<Restaurant> restaurantsList;


      // if(currentTime.isAfter(morningStartTime) && currentTime.isBefore(morningEndTime) || 
      // currentTime.isAfter(afternoonStartTime) && currentTime.isBefore(agternoonEndTime) ||
      // currentTime.isAfter(eveningStartTime) && currentTime.isBefore(eveningEndTime)){
      //   restaurantsList = restaurantRepositoryService.findAllRestaurantsCloseBy(latitude, longitude, currentTime, peakHoursServingRadiusInKms);
      // }
      // else{
      //   restaurantsList = restaurantRepositoryService.findAllRestaurantsCloseBy(latitude, longitude, currentTime, normalHoursServingRadiusInKms);
      // }
      // restaurantRepositoryService.findAllRestaurantsCloseBy(latitude, longitude, currentTime, servingRadiusInKms)
      
      // call the helper method setCorrectServingRadiusMethod
      setCorrectServingRadius(getRestaurantsRequest, currentTime);
      
      restaurantsList = restaurantRepositoryService.findAllRestaurantsCloseBy(latitude, longitude, currentTime,correctServingRadius);
      GetRestaurantsResponse getRestaurantsResponse = new GetRestaurantsResponse(restaurantsList);
      // long endTimeInMillis = System.currentTimeMillis();
    
      // System.out.println("Serice layer took :" + (endTimeInMillis - startTimeInMillis));
      return getRestaurantsResponse;
    }
    //  return null;
    
    // @Override
  // public GetRestaurantsResponse findAllRestaurantsCloseBy(
  //     GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {


  // }


  // }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
        
        List<Restaurant> restaurantList = new ArrayList<>();
        String searchQuery = getRestaurantsRequest.getSearchFor();
        if(searchQuery=="")
          return new GetRestaurantsResponse(restaurantList);

        Double latitude = getRestaurantsRequest.getLatitude();
        Double longitude = getRestaurantsRequest.getLongitude();
        setCorrectServingRadius(getRestaurantsRequest, currentTime);

        // ExecutorService threadPool = Executors.newFixedThreadPool(4);
        
        // threadPool.execute(() -> {
        //   restaurantList.addAll(restaurantRepositoryService.findRestaurantsByName(latitude, longitude, searchQuery, currentTime, correctServingRadius));
        // });
        
        // threadPool.execute(() -> {
          //   restaurantList.addAll(restaurantRepositoryService.findRestaurantsByAttributes(latitude, longitude, searchQuery, currentTime, correctServingRadius));
          // });
          
          // threadPool.execute(() -> {
            //   restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemName(latitude, longitude, searchQuery, currentTime, correctServingRadius));
            // });
            
            // threadPool.execute(() -> {
              //   restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemAttributes(latitude, longitude, searchQuery, currentTime, correctServingRadius));
              // });
              
              // try {
                //   Thread.sleep(2000);
                // } catch (InterruptedException e) {
                  //   // TODO Auto-generated catch block
                  //   e.printStackTrace();
                  // }
                  // threadPool.execute(command);
                  
                  
          restaurantList.addAll(restaurantRepositoryService.findRestaurantsByName(latitude, longitude, searchQuery, currentTime, correctServingRadius));
          restaurantList.addAll(restaurantRepositoryService.findRestaurantsByAttributes(latitude, longitude, searchQuery, currentTime, correctServingRadius));
          restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemName(latitude, longitude, searchQuery, currentTime, correctServingRadius));
          restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemAttributes(latitude, longitude, searchQuery, currentTime, correctServingRadius));
          return new GetRestaurantsResponse(restaurantList);
  }

  public void setCorrectServingRadius(GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime){

      Double latitude = getRestaurantsRequest.getLatitude();
      Double longitude = getRestaurantsRequest.getLongitude();

      LocalTime morningStartTime = LocalTime.of(7, 59);
      LocalTime morningEndTime = LocalTime.of(10, 01);
      LocalTime afternoonStartTime = LocalTime.of(12, 59);
      LocalTime agternoonEndTime = LocalTime.of(14, 01);
      LocalTime eveningStartTime = LocalTime.of(18, 59);
      LocalTime eveningEndTime = LocalTime.of(21, 01);
      
      if(currentTime.isAfter(morningStartTime) && currentTime.isBefore(morningEndTime) || 
      currentTime.isAfter(afternoonStartTime) && currentTime.isBefore(agternoonEndTime) ||
      currentTime.isAfter(eveningStartTime) && currentTime.isBefore(eveningEndTime)){
        correctServingRadius = peakHoursServingRadiusInKms;        
      }
      else{
        correctServingRadius = normalHoursServingRadiusInKms;
        // restaurantsList = restaurantRepositoryService.findAllRestaurantsCloseBy(latitude, longitude, currentTime, normalHoursServingRadiusInKms);
      }

  }

  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    // TODO Auto-generated method stub

    List<Restaurant> restaurantList = new ArrayList<>();
    String searchQuery = getRestaurantsRequest.getSearchFor();
    if(searchQuery=="")
      return new GetRestaurantsResponse(restaurantList);

    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    setCorrectServingRadius(getRestaurantsRequest, currentTime);

    ExecutorService threadPool = Executors.newFixedThreadPool(4);
    
    threadPool.execute(() -> {
      restaurantList.addAll(restaurantRepositoryService.findRestaurantsByName(latitude, longitude, searchQuery, currentTime, correctServingRadius));
    });
    
    threadPool.execute(() -> {
      restaurantList.addAll(restaurantRepositoryService.findRestaurantsByAttributes(latitude, longitude, searchQuery, currentTime, correctServingRadius));
    });
    
    threadPool.execute(() -> {
      restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemName(latitude, longitude, searchQuery, currentTime, correctServingRadius));
    });
    
    threadPool.execute(() -> {
      restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemAttributes(latitude, longitude, searchQuery, currentTime, correctServingRadius));
    });

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return new GetRestaurantsResponse(restaurantList);
  }

}

