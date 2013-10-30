/*
 *   This software is licensed under the Apache 2 license, quoted below.
 *
 *   Copyright 2012-2013 Martin Bednar
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *   use this file except in compliance with the License. You may obtain a copy of
 *   the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations under
 *   the License.
 */

package org.elasticsearch.river.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import au.com.bytecode.opencsv.CSVReader;

/**
 *
 */
public class CSVRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private String indexName;
    private String typeName;
    private int bulkSize;
    private String folderName;
    private String filenamePattern;
    private volatile BulkRequestBuilder currentRequest;
    private volatile boolean closed = false;
    private List<Object> csvFields;
    private TimeValue poll;
    private Thread thread;
    private char escapeCharacter;
    private char quoteCharacter;
    private char separator;
    private AtomicInteger onGoingBulks = new AtomicInteger();
    private int bulkThreshold;

    @Inject
    public CSVRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
        
        logger.info("creating csv stream river {}", riverName.getName());
    }

    @Override
    public void start() {
        logger.info("starting csv stream");
        currentRequest = client.prepareBulk();
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "CSV processor").newThread(new CSVConnector());
        thread.start();
    }

    @Override
    public void close() {
        logger.info("closing csv stream river");
        this.closed = true;
        try {
			thread.join(3000);
			thread.interrupt();
		} catch (InterruptedException e) {
		}
    }

    private void delay() {
        if (poll.millis() > 0L) {
            logger.info("next run waiting for {}", poll);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e) {
            	if(!closed)
            	{
            		logger.error("Error during waiting.", e, (Object) null);
            	}
            }
        }
        else
        {
        	logger.info("No delay set, shutting down.");
        }
    }

    private synchronized void notifyThread()
    {
    	this.notify();
    }

    private void processBulkIfNeeded(boolean force, final long startAt) {
        if (currentRequest.numberOfActions() >= bulkSize || (force && currentRequest.numberOfActions()>0)) {
            // execute the bulk operation
            int currentOnGoingBulks = onGoingBulks.incrementAndGet();
            if (currentOnGoingBulks > bulkThreshold) {
                onGoingBulks.decrementAndGet();
                logger.warn("ongoing bulk, [{}] crossed threshold [{}], waiting", onGoingBulks, bulkThreshold);
                try {
                    synchronized (this) {
                        wait();
                    }
                } catch (InterruptedException e) {
                	if(!closed)
                	{
                		logger.error("Error during wait", e);
                	}
                }
            }
            
            try {
            	logger.info("executing bulk");
                currentRequest.execute(new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        onGoingBulks.decrementAndGet();
                        notifyThread();
                        if(bulkResponse.hasFailures())
                        {
                        	BulkItemResponse[] items=bulkResponse.getItems();
                        	for(int ii=0;ii<items.length;ii++)
                        	{
                        		if(items[ii].isFailed())
                        		{
                        			logger.error("Failure at line {}:{}", startAt+(long)ii, items[ii].getFailure().getMessage());
                        		}
                        	}
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        onGoingBulks.decrementAndGet();
                        notify();
                        logger.error("failed to execute bulk");
                    }
                });
            } catch (Exception e) {
                onGoingBulks.decrementAndGet();
                notifyThread();
                logger.error("failed to process bulk", e);
            }
            currentRequest = client.prepareBulk();
        }
    }

    private class CSVConnector implements Runnable {

    	@SuppressWarnings({"unchecked"})
        @Override
        public void run() {
        	Reader reader=null;
        	
        	csvFields = XContentMapValues.extractRawValues("fields", settings.settings());
            poll = XContentMapValues.nodeTimeValue(settings.settings().get("poll"), TimeValue.timeValueMinutes(0));
            escapeCharacter = XContentMapValues.nodeStringValue(settings.settings().get("escape_character"), String.valueOf(CSVReader.DEFAULT_ESCAPE_CHARACTER)).charAt(0);
            separator = XContentMapValues.nodeStringValue(settings.settings().get("field_separator"), String.valueOf(CSVReader.DEFAULT_SEPARATOR)).charAt(0);
            quoteCharacter = XContentMapValues.nodeStringValue(settings.settings().get("quote_character"), String.valueOf(CSVReader.DEFAULT_QUOTE_CHARACTER)).charAt(0);
        	
            if (settings.settings().containsKey("index")) {
                Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
                indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
                typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "csv_type");
                bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
                bulkThreshold = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_threshold"), 10);
            } else {
                indexName = "csv_"+riverName.name();
                typeName = "csv_type";
                bulkSize = 100;
                bulkThreshold = 10;
            }
            
        	if(settings.settings().containsKey("csv_string"))
            {
            	try {
            		reader=new BufferedReader(new StringReader(XContentMapValues.nodeStringValue(settings.settings().get("csv_string"), null)));
					processReader(reader,"csv_string");
				} catch (IOException e) {
					logger.error("Error processing csv_string", e);
				}
            }
        	else if(settings.settings().containsKey("csv_data"))
            {
				try {
					byte[] decoded = Base64.decode(XContentMapValues.nodeStringValue(settings.settings().get("csv_data"), null));
					reader=new BufferedReader(new StringReader(new String(decoded,"UTF-8")+"\n"));
	            	processReader(reader,"csv_data");
				} catch (IOException e) {
					logger.error("Error processing csv_data", e);
				}
            }
            else if (settings.settings().containsKey("csv_file")) {
            	logger.info("found csv_file");
                Map<String, Object> csvSettings = (Map<String, Object>) settings.settings().get("csv_file");
                folderName = XContentMapValues.nodeStringValue(csvSettings.get("folder"), null);
                filenamePattern = XContentMapValues.nodeStringValue(csvSettings.get("filename_pattern"), ".*\\.csv$");
                
                File lastProcessedFile = null;
                try {
                    File files[] = getFiles();
                    for (File file : files) {
                    	if(closed)
                    	{
                    		break;
                    	}
                    	
                    	try
                    	{
	                        logger.info("Processing file {}", file.getName());
	                        file = renameFile(file, ".processing");
	                        lastProcessedFile = file;
	
	                        processReader(new BufferedReader(new FileReader(file)),file.getAbsolutePath());
	
	                        file = renameFile(file, ".imported");
	                        lastProcessedFile = file;
                    	}
                    	catch (Exception e) {
                            if (lastProcessedFile != null) {
                                renameFile(lastProcessedFile, ".error");
                                logger.error("Error processing file {}", e,lastProcessedFile);
                            }
                            else
                            {
                            	logger.error("Error processing file {}", e,file);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (lastProcessedFile != null) {
                        renameFile(lastProcessedFile, ".error");
                    }
                    logger.error(e.getMessage(), e, (Object) null);
                    closed = true;
                }
                if (closed) {
                    return;
                }
            }
            else if(settings.settings().containsKey("csv_uri"))
            {
            	CloseableHttpClient httpClient=HttpClients.createDefault();
            	String uri=XContentMapValues.nodeStringValue(settings.settings().get("csv_uri"), null);
            	HttpGet get=new HttpGet(uri);
            	CloseableHttpResponse response=null;
            	try
            	{
            		response=httpClient.execute(get);
            		reader=new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            		processReader(reader,uri);
				} catch (Exception e) {
					logger.error("Error invoking URI",e);
				}
            	finally
            	{
            		try
            		{
            			response.close();
            		}
            		catch(IOException e)
            		{
            			logger.error("Could not close HTTP response", e);
            		}
            	}
            }
            else
            {
            	logger.warn("No data to process.");
            }
            
        	logger.info("done processing");
            delay();
        }

        private File renameFile(File file, String suffix) {
            File newFile = new File(file.getAbsolutePath() + suffix);
            if (!file.renameTo(newFile)) {
                logger.error("can't rename file {} to {}", file.getName(), newFile.getName());
            }
            return newFile;
        }

        private File[] getFiles() {
            File folder = new File(folderName);
            return folder.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File file, String s) {
                    return s.matches(filenamePattern);
                }
            });
        }

        private void processReader(Reader reader, String streamName) throws IOException {
        	long line=0;
            CSVReader csv = new CSVReader(reader, separator, quoteCharacter, escapeCharacter);
            String[] nextLine;
            try
            {
	            while ( !closed && (nextLine = csv.readNext()) != null) {
	            	line++;
	            	if(nextLine.length==csvFields.size()) {
	                    XContentBuilder builder = XContentFactory.jsonBuilder();
	                    builder.startObject();
	
	                    for(int ii=0;ii<csvFields.size();ii++)
	                    {
	                    	builder.field((String) csvFields.get(ii), nextLine[ii]);
	                    }
	                    
	                    builder.endObject();
	                    logger.trace("Adding request to bulk");
	                    currentRequest.add(Requests.indexRequest(indexName).type(typeName).source(builder));
	                }
	            	else
	            	{
	            		logger.warn("Could not process line {}:{}, found {} fields, expecting {}",streamName,line,nextLine.length,csvFields.size());
	            	}
	                processBulkIfNeeded(false,line);
	            }
	            processBulkIfNeeded(true,line);
            }
            catch(IOException e)
            {
            	throw new IOException(e);
            }
            finally
            {
            	try
            	{
            		csv.close();
            	}
            	catch(IOException ie)
            	{
            		logger.error("Failed to close CSVReader", ie);
            	}
            }
        }
    }
}
