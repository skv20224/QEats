/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import redis.clients.jedis.Jedis;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Item;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
// import redis.clients.jedis.Jedis;





@Primary
@Service
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {
  
  @Autowired
  private RedisConfiguration redisConfiguration;
  
  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  @Autowired
  private RestaurantRepository restaurantRepository;

  @Autowired
  private ItemRepository itemRepository;

  @Autowired
  private MenuRepository menuRepository;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }


  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  @Override
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {
            
    // long startTimeInMillis = System.currentTimeMillis();
    
    List<Restaurant> restaurants = null;

    // System.out.println(redisConfiguration.isCacheAvailable());
    if(redisConfiguration.isCacheAvailable()){
      restaurants = findAllRestaurantsCloseByFromCache(latitude, longitude, currentTime, servingRadiusInKms);
    }else{
      restaurants = findAllRestaurantsCloseByFromDb(latitude, longitude, currentTime, servingRadiusInKms);
    }


    

    // Jedis jedis = redisConfiguration.getJedisPool().getResource();
    
    // String key = String.valueOf(latitude) + " " + String.valueOf(longitude) + " " + " " + String.valueOf(servingRadiusInKms);

    // if(jedis.exists(key)){
    //   // Restaurant[] res = new ObjectMapper().readValue(jedis.get(key), Restaurant[].class);
    //   // System.out.println(jedis.get(key));
    //  Restaurant[] res = modelMapperProvider.get().map(jedis.get(key),  Restaurant[].class);
    //  restaurants = Arrays.asList(res);
    //  return restaurants;
    // }

    // Old method before redis cache
    // List<RestaurantEntity> restaurantEntityList = restaurantRepository.findAll();
    
    // for(RestaurantEntity restaurantEntity : restaurantEntityList){
    
    //   if(isRestaurantCloseByAndOpen(restaurantEntity, currentTime, latitude, longitude, servingRadiusInKms) ){
    //     Restaurant obj = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);
        
    //     restaurants.add(obj);
    //   }
    // }
    // long endTimeInMillis = System.currentTimeMillis();

    // System.out.println("Repository layer took :" + (endTimeInMillis - startTimeInMillis));
    return restaurants;
    
  }






  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objective:
  // 1. Check if a restaurant is nearby and open. If so, it is a candidate to be returned.
  // NOTE: How far exactly is "nearby"?

  private List<Restaurant> findAllRestaurantsCloseByFromDb(Double latitude, Double longitude,
      LocalTime currentTime, Double servingRadiusInKms) {
    
    List<Restaurant> restaurants = new ArrayList<>();
    List<RestaurantEntity> restaurantEntities = restaurantRepository.findAll();
    ModelMapper modelMapper = modelMapperProvider.get();
    for(RestaurantEntity restaurantEntity : restaurantEntities){
        if(isRestaurantCloseByAndOpen(restaurantEntity, currentTime, latitude, longitude, servingRadiusInKms))
            restaurants.add(modelMapper.map(restaurantEntity, Restaurant.class));
        
    }
      return restaurants;
  }


  private List<Restaurant> findAllRestaurantsCloseByFromCache(Double latitude, Double longitude,
      LocalTime currentTime, Double servingRadiusInKms) {
      
      List<Restaurant> restaurants = new ArrayList<>();
      // GeoLocation geoLocation = new GeoLocation(latitude, longitude);
      GeoHash geoHash = GeoHash.withCharacterPrecision(latitude, longitude, 7);
       try (Jedis jedis = redisConfiguration.getJedisPool().getResource()) {

          String cacheResponse = jedis.get(geoHash.toBase32());
          
          if(cacheResponse == null){
            restaurants = findAllRestaurantsCloseByFromDb(latitude, longitude, currentTime, servingRadiusInKms);
            jedis.setex(geoHash.toBase32(), GlobalConstants.REDIS_ENTRY_EXPIRY_IN_SECONDS , new ObjectMapper().writeValueAsString(restaurants));
          }
          else{
            restaurants = new ObjectMapper().readValue(cacheResponse, new TypeReference<List<Restaurant>>(){});
          }

       } catch (Exception e) {
        //TODO: handle exception
       } 
        return restaurants;
      }
      
  // public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
  //     Double longitude, LocalTime currentTime, Double servingRadiusInKms) {

  //   List<Restaurant> restaurants = null;




  //   return restaurants;
  // }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

      // List<RestaurantEntity> restaurants = restaurantRepository.findByName(searchString);
      Optional<List<RestaurantEntity>> restaurants1 = restaurantRepository.findRestaurantsByNameExact(searchString);
      
      // this is for partial match
      Optional<List<RestaurantEntity>> restaurants2 = restaurantRepository.findRestaurantsByName(searchString);  
      // List<Restaurant> restaurantsList = new ArrayList<>();
      List<Restaurant> restaurantsList = null;
      
      if(restaurants1.isPresent()){
        restaurantsList = new ArrayList<>();
        for(RestaurantEntity res : restaurants1.get()){
          if(isRestaurantCloseByAndOpen(res, currentTime, latitude, longitude, servingRadiusInKms))
            restaurantsList.add( modelMapperProvider.get().map(res,Restaurant.class));
        }
    }
    
    if(restaurants2.isPresent()){
      // restaurantsList = new ArrayList<>();
      for(RestaurantEntity res : restaurants2.get()){
        if(isRestaurantCloseByAndOpen(res, currentTime, latitude, longitude, servingRadiusInKms))
          restaurantsList.add( modelMapperProvider.get().map(res,Restaurant.class));
      }
  }


     return restaurantsList;
    }
    
    
    // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
    // Objective:
    // Find restaurants whose attributes (cuisines) intersect with the search query.
    @Override
    public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
        
      // Optional<List<RestaurantEntity>> restaurants = restaurantRepository.findRestaurantsByAtributeExact(searchString);
        
      // List<Restaurant> restaurantsList = new ArrayList<>();
      List<Restaurant> restaurantsList = findAllRestaurantsCloseBy(latitude, longitude, currentTime, servingRadiusInKms);
      
      List<Restaurant> answer = new ArrayList<>();
      
      // searchString.equalsIgnoreCase(anotherString)
      for(Restaurant res : restaurantsList){
        if(res.getAttributes().contains(searchString))
          answer.add(res);
      }
    
    return answer;
        
    
    //  return null;
  }
  
  
  
  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.
  
  @Override
  public List<Restaurant> findRestaurantsByItemName(
    Double latitude, Double longitude,
    String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    // Exact Match
    Optional<List<ItemEntity>> l1 = itemRepository.findItemsByNameExact(searchString);
    
    // Partial Match
    Optional<List<ItemEntity>> l2 = itemRepository.findItemsByName(searchString);
    
    // Optional<List<ItemEntity>> itemList =  itemRepository.findByItemName(searchString);
    // if(itemList.isPresent()){
      
    // }
    List<ItemEntity> itemList = new ArrayList<>();
    if(l1.isPresent()){
      itemList.addAll(l1.get());
    }

    if(l2.isPresent()){
      itemList.addAll(l2.get());
    }
      // itemList.

    List<String> itemIdList = itemList.stream().map(item-> item.getItemId()).collect(Collectors.toList());
    
    Optional<List<MenuEntity>> menuListOptional = menuRepository.findMenusByItemsItemIdIn(itemIdList);
    Optional<List<RestaurantEntity>> restaurantEnitityList = Optional.empty();
    // List<String> restaurantIdList;
    if(menuListOptional.isPresent()){
      List<MenuEntity> menuEntities = menuListOptional.get();
      List<String>  restaurantIdList = menuEntities.stream().map(MenuEntity::getRestaurantId).collect(Collectors.toList());
      restaurantEnitityList = restaurantRepository.findRestaurantsByRestaurantIdIn(restaurantIdList);
    }

    if(restaurantEnitityList.isPresent()){
        List<Restaurant> answer = new ArrayList<>();
        for(RestaurantEntity res : restaurantEnitityList.get()){
          if(isRestaurantCloseByAndOpen(res, currentTime, latitude, longitude, servingRadiusInKms))
            answer.add( modelMapperProvider.get().map(res,Restaurant.class));
          }
          return answer;
    }
    
    return null;


  }
  
  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
  String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    List<String> itemIdList = new ArrayList<>();
    List<ItemEntity> itemList = itemRepository.findAll();
    for(ItemEntity it : itemList){
      if(it.getAttributes().contains(searchString)){
        itemIdList.add(it.getItemId());
      }
    }

    Optional<List<MenuEntity>> menuListOptional = menuRepository.findMenusByItemsItemIdIn(itemIdList);

    Optional<List<RestaurantEntity>> restaurantEnitityList = Optional.empty();
    // List<String> restaurantIdList;
    if(menuListOptional.isPresent()){
      List<MenuEntity> menuEntities = menuListOptional.get();
      List<String>  restaurantIdList = menuEntities.stream().map(MenuEntity::getRestaurantId).collect(Collectors.toList());
      restaurantEnitityList = restaurantRepository.findRestaurantsByRestaurantIdIn(restaurantIdList);
    }

    if(restaurantEnitityList.isPresent()){
        List<Restaurant> answer = new ArrayList<>();
        for(RestaurantEntity res : restaurantEnitityList.get()){
          if(isRestaurantCloseByAndOpen(res, currentTime, latitude, longitude, servingRadiusInKms))
            answer.add( modelMapperProvider.get().map(res,Restaurant.class));
          }
          return answer;
    }
    
    return null;


    
  }





  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude, restaurantEntity.getLatitude(),
          restaurantEntity.getLongitude()) <= servingRadiusInKms;
    }

    return false;
  }



}

