package main.java.uk.ac.cam.cares.jps.agent.Hawker;


import org.json.JSONArray;
import org.json.JSONObject;
import org.jooq.exception.DataAccessException;
import uk.ac.cam.cares.jps.base.util.JSONKeyToIRIMapper;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesClient;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesSparql;
import me.xdrop.fuzzywuzzy.FuzzySearch;
import java.text.SimpleDateFormat;
import uk.ac.cam.cares.jps.base.exception.JPSRuntimeException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import javax.lang.model.util.ElementScanner6;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.rdf4j.sparqlbuilder.core.Prefix;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;

import org.json.JSONArray;

import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import static org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf.iri;

import org.eclipse.rdf4j.sparqlbuilder.core.query.DeleteDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.InsertDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;


public class APIQueryBuilder
{
    

    public String queryEndpoint;
    public String updateEndpoint;

    RemoteStoreClient kbClient;

    /**
     * Namespaces for ontologies
     */

    public static final String OntoHawker = "https://www.theworldavatar.com/kg/ontohawker/";
    public static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";

    
    
	/**
     * Prefixes
     */ 

    private static final Prefix PREFIX_ONTOHAWKER = SparqlBuilder.prefix("ontoHawker", iri(OntoHawker));
    public static final String generatedIRIPrefix = TimeSeriesSparql.TIMESERIES_NAMESPACE + "Hawker";
    private static final Prefix PREFIX_RDFS = SparqlBuilder.prefix("rdfs", iri(RDFS_NS));

    
    
	/**
     * Relationships
    */ 

    private static final Iri hasAddress = PREFIX_ONTOHAWKER.iri("hasAddress");
    private static final Iri hasGrading = PREFIX_ONTOHAWKER.iri("hasGrading");
    private static final Iri hasStall = PREFIX_ONTOHAWKER.iri("hasStall");
    private static final Iri hasType = PREFIX_ONTOHAWKER.iri("hasType");
    private static final Iri hasOwner = PREFIX_ONTOHAWKER.iri("hasOwner");
    private static final Iri hasLabel = PREFIX_RDFS.iri("hasLabel");  
    private static final Iri hasTotalStalls = PREFIX_ONTOHAWKER.iri("hasTotalStalls");
    private static final Iri hasFoodStalls = PREFIX_ONTOHAWKER.iri("hasFoodStalls");
    private static final Iri hasMarketProduceStalls = PREFIX_ONTOHAWKER.iri("hasMarketProduceStalls");
    
    
    
    private static final Iri hasID = PREFIX_ONTOHAWKER.iri("hasID");
    private static final Iri hasStallAddress = PREFIX_ONTOHAWKER.iri("hasStallAddress");
    private static final Iri hasLicenseNumber = PREFIX_ONTOHAWKER.iri("hasLicenseNumber");
    private static final Iri hasLicenseeName = PREFIX_ONTOHAWKER.iri("hasLicenseeName");
    private static final Iri hasStallGrading = PREFIX_ONTOHAWKER.iri("hasStallGrading");
    
    

    

    /**
     * Classes
    */

  
    private static final Iri Label = PREFIX_ONTOHAWKER.iri("Label");
    private static final Iri Hawker = PREFIX_ONTOHAWKER.iri("Hawker");
    private static final Iri Address = PREFIX_ONTOHAWKER.iri("Address");
    private static final Iri Stall = PREFIX_ONTOHAWKER.iri("Stall");
    private static final Iri Grading = PREFIX_ONTOHAWKER.iri("Grading");
    private static final Iri Type = PREFIX_ONTOHAWKER.iri("Type");
    private static final Iri Owner = PREFIX_ONTOHAWKER.iri("Owner");
    
    private static final Iri TotalStalls = PREFIX_ONTOHAWKER.iri("TotalStalls");
    private static final Iri FoodStalls = PREFIX_ONTOHAWKER.iri("FoodStalls");
    private static final Iri MarketProduceStalls = PREFIX_ONTOHAWKER.iri("FoodStalls");
    

    private static final Iri StallAddress = PREFIX_ONTOHAWKER.iri("StallAddress");
    private static final Iri LicenseNumber = PREFIX_ONTOHAWKER.iri("LicenseNumber");
    private static final Iri LicenseeName = PREFIX_ONTOHAWKER.iri("LicenseeName");
    private static final Iri StallGrading = PREFIX_ONTOHAWKER.iri("StallGrading");
    




    public String agentProperties;
    public String clientProperties;

    public JSONObject hawkerReadings;
    public JSONObject stallsReadings;

    

    private List<JSONKeyToIRIMapper> mappings;

    public APIQueryBuilder(String agentProp, String clientProp) throws IOException
    {
        agentProperties = agentProp;
        clientProperties = clientProp;


        loadconfigs(clientProperties);
        //readings endpoints from client.properties

        loadproperties(agentProperties);
        
        kbClient = new RemoteStoreClient();

        kbClient.setUpdateEndpoint(updateEndpoint);
        kbClient.setQueryEndpoint(queryEndpoint);


    }

    public void loadproperties(String propfile) throws IOException
    {
        try(InputStream input = new FileInputStream(propfile))
        {
            Properties prop = new Properties();
            prop.load(input);

            String mappingfolder;

            try
            {
                mappingfolder = System.getenv(prop.getProperty("Hawker.mappingfolder"));
            }
            catch(NullPointerException e)
            {
                throw new IOException("The key Hawker.mappingfolder cannot be found");
            }

            if(mappingfolder == null)
            {
                throw new InvalidPropertiesFormatException("The properties file does not contain the key School.mappingfolder with a path to the folder containing the required JSON key to IRI Mappings");
            }

            mappings = new ArrayList<>();
            File folder = new File(mappingfolder);
            File[] mappingFiles = folder.listFiles();

            if(mappingFiles.length == 0)
            {
                throw new IOException("No files in folder");
            }

            else
            {
                for(File mappingFile : mappingFiles)
                {
                    JSONKeyToIRIMapper mapper = new JSONKeyToIRIMapper(generatedIRIPrefix, mappingFile.getAbsolutePath());
                    mappings.add(mapper);
                    mapper.saveToFile(mappingFile.getAbsolutePath());
                }
            }

        }


    }

    public void loadconfigs(String filepath) throws IOException
    {
        File file = new File(filepath);
        if(!file.exists())
        {
            throw new FileNotFoundException("There was no file found in the path");
        }
        
        try(InputStream input = new FileInputStream(file))
        {
            Properties prop = new Properties();
            prop.load(input);

            if(prop.containsKey("sparql.query.endpoint"))
            {
                queryEndpoint = prop.getProperty("sparql.query.endpoint");
            }
            else
            {
                throw new IOException("The file is missing: \"sparql.query.endpoint=<queryEndpoint>\"");
            }

            if(prop.containsKey("sparql.update.endpoint"))
            {
                updateEndpoint = prop.getProperty("sparql.update.endpoint");
            }
            else
            {
                throw new IOException("The file is missing: \"sparql.update.endpoint=<updateEndpoint>\"");
            }
        }
    }


    public void instantiateIfNotInstantiated(JSONObject hawker, JSONObject stalls)
    {
        
        hawkerReadings = hawker;
        stallsReadings = stalls;

        

        List<String> iris;
        

        JSONArray hawkerArray = hawkerReadings.getJSONObject("result").getJSONArray("records");

        for(int j=0;j<hawkerArray.length();j++)
        {
            JSONObject curHawkerCentre = hawkerReadings.getJSONObject(j);
            String hawkerName = curHawkerCentre.getString("name_of_centre");

            String result=null;
            Variable hawkerIRI =SparqlBuilder.var("hawkerIRI");
            SelectQuery q = Queries.SELECT();

            TriplePattern qp = hawkerIRI.isA(Hawker).andHas(hasLabel,hawkerName);
            q.prefix(PREFIX_ONTOHAWKER).select(hawkerIRI).where(qp);
            kbClient.setQuery(q.getQueryString());

            try
            {
                JSONArray queryResult = kbClient.executeQuery();
                  
                if(!queryResult.isEmpty())
                {
                    result = kbClient.executeQuery().getJSONObject(0).getString("hawkerIRI");
                }
                else
                {
                    //if queryresult is empty 
                    result = OntoHawker + hawkerName + "_" + UUID.randomUUID(); 
                }

            }
            catch(Exception e)
            {
                throw new JPSRuntimeException("Unable to execute query: " + q.getQueryString());
            }

            TriplePattern pattern = iri(result).isA(Hawker);
            InsertDataQuery insert1 = Queries.INSERT_DATA(pattern);
            insert1.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insert1.getQueryString());

            TriplePattern namePattern = iri(result).has(hasLabel,hawkerName);
            InsertDataQuery insertname = Queries.INSERT_DATA(namePattern);
            insertname.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insertname.getQueryString());

            String hawkerType = curHawkerCentre.getString("type_of_centre");
            String hawkerOwner = curHawkerCentre.getString("owner");

            TriplePattern updateType = iri(result).has(hasType,hawkerType);
            InsertDataQuery insertType = Queries.INSERT_DATA(updateType);
            insertType.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insertType.getQueryString());

            
            TriplePattern updateOwner = iri(result).has(hasOwner,hawkerOwner);
            InsertDataQuery insertOwner = Queries.INSERT_DATA(updateOwner);
            insertOwner.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insertOwner.getQueryString());

            
            
            String add = curHawkerCentre.getString("location_of_centre");
            String numberofStalls = curHawkerCentre.getString("no_of_stalls");
            String foodStalls = curHawkerCentre.getString("no_of_cooked_food_stalls");
            String marketProduceStalls = curHawkerCentre.getString("no_of_mkt_produce_stalls");



            //TriplePattern for address
            TriplePattern pattern4 = iri(result).has(hasAddress,add);
            InsertDataQuery insert4 = Queries.INSERT_DATA(pattern4);
            insert4.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insert4.getQueryString());

            //TriplePattern for total number of stalls
            TriplePattern pattern5 = iri(result).has(hasTotalStalls,numberofStalls);
            InsertDataQuery insert5 = Queries.INSERT_DATA(pattern5);
            insert5.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insert5.getQueryString());

            //TriplePattern for total food stalls
            TriplePattern patternFoodStalls = iri(result).has(hasFoodStalls,foodStalls);
            InsertDataQuery insertFoodStalls = Queries.INSERT_DATA(patternFoodStalls);
            insertFoodStalls.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insertFoodStalls.getQueryString());


            //TriplePattern for total market produce stalls
            TriplePattern patternmarket = iri(result).has(hasMarketProduceStalls,marketProduceStalls);
            InsertDataQuery insertMarketStalls = Queries.INSERT_DATA(patternmarket);
            insertMarketStalls.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insertMarketStalls.getQueryString());




            //FuzzyMatching for the Avergae Grading of a Hawker Centre;
            int totalScore=0, count=0;
            
            try
            {
                JSONArray jsArr = stallsReadings.getJSONObject("result").getJSONArray("records");
                for(int i=0; i<jsArr.length();i++)
                {
                    JSONObject currentStall = jsArr.getJSONObject(i);
                    String current = currentStall.getString("premises_address");

                    if(FuzzySearch.tokenSetRatio(current.toLowerCase(),hawkerName.toLowerCase())>90 &&
                    FuzzySearch.partialRatio(current.toLowerCase(),hawkerName.toLowerCase())>75 && FuzzySearch.tokenSortRatio(current.toLowerCase(),hawkerName.toLowerCase())>83)
                    {
                        count++;
                        String score = currentStall.getString("grade");
                        if(score == "A")
                        totalScore += 90;
                        else if(score == "B")
                        totalScore += 80;
                        else
                        totalScore += 70;
                    

                        String stallRes=null;
                        Variable stallIRI = SparqlBuilder.var("stallIRI");
                        SelectQuery q1 = Queries.SELECT();

                        String stallID = currentStall.getString("_id");
                        TriplePattern qp1 = stallIRI.isA(Stall).andHas(hasID,stallID);
                        q1.prefix(PREFIX_ONTOHAWKER).select(stallIRI).where(qp1);
                        kbClient.setQuery(q1.getQueryString());

                        try
                        {
                            JSONArray queryResult1 = kbClient.executeQuery();
                            if(!queryResult1.isEmpty())
                            {
                                stallRes = kbClient.executeQuery().getJSONObject(0).getString("stallIRI");
                            }
                            else
                            {
                                stallRes = OntoHawker + stallID + "_" + UUID.randomUUID();
                            }
                        }
                        catch(Exception e)
                        {
                            throw new JPSRuntimeException("Unable to execute query: " + q1.getQueryString());
                        }
                        TriplePattern patternUpdate = iri(stallRes).isA(Stall);
                        InsertDataQuery insertUpdate1 = Queries.INSERT_DATA(patternUpdate);
                        insertUpdate1.prefix(PREFIX_ONTOHAWKER);
                        kbClient.executeUpdate(insertUpdate1.getQueryString());

                        //TriplePattern to instantiate this stall as part of the current HawkerCentre

                        TriplePattern patternStall = iri(result).has(hasStall,stallRes);
                        InsertDataQuery insertStall = Queries.INSERT_DATA(patternStall);
                        insertStall.prefix(PREFIX_ONTOHAWKER);
                        kbClient.executeUpdate(insertStall.getQueryString());


                        String licName = currentStall.getString("licensee_name");
                        String licNo = currentStall.getString("licence_number");
                        String grade = currentStall.getString("grade");

                        TriplePattern patternLicName = iri(stallRes).has(hasLicenseeName,licName);
                        InsertDataQuery insertLicName = Queries.INSERT_DATA(patternLicName);
                        insertLicName.prefix(PREFIX_ONTOHAWKER);
                        kbClient.executeUpdate(insertLicName.getQueryString());

                        TriplePattern patternLicNo = iri(stallRes).has(hasLicenseNumber,licNo);
                        InsertDataQuery insertLicNo = Queries.INSERT_DATA(patternLicNo);
                        insertLicNo.prefix(PREFIX_ONTOHAWKER);
                        kbClient.executeUpdate(insertLicNo.getQueryString());

                        TriplePattern patterngrade = iri(stallRes).has(hasStallGrading,grade);
                        InsertDataQuery insertGrade = Queries.INSERT_DATA(patterngrade);
                        insertGrade.prefix(PREFIX_ONTOHAWKER);
                        kbClient.executeUpdate(insertGrade.getQueryString());


                    }

                }

            }
            catch(Exception e)
            {
                throw new JPSRuntimeException("Readings can not be empty!", e);
            }
            String grading;
            if(count!=0)
            {
                int avgScore = totalScore/count;
               
                if(avgScore>85)
                grading="A";
                else if(avgScore<75)
                grading="C";
                else
                grading="C";
            }
            else
            grading="No gradings available";

            TriplePattern updateGrading = iri(result).has(hasGrading,grading);
            InsertDatQuery insertGrading = Queries.INSERT_DATA(updateGrading);
            insertGrading.prefix(PREFIX_ONTOHAWKER);
            kbClient.executeUpdate(insertGrading.getQueryString());

            
        }
    }
}
