package com.chimpler.hive.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrTableCursor {
	private HttpSolrServer server;
	private int start;
	private int count;
	private int numBufferRows;
	private String[] fields;

	private SolrDocumentList buffer;
	private int pos = 0;
	private long numFound;

	SolrTableCursor(String url, String[] fields, int start, int count, int numBufferRows) throws IOException {
        server = new HttpSolrServer(url);
        this.fields = fields;
        this.start = start;
        this.count = count;
        
        this.numBufferRows = numBufferRows;
        fetchNextDocumentChunk();
	}

	private void fetchNextDocumentChunk() throws IOException {
        SolrQuery query = new SolrQuery();
        query.setStart(start);
        query.setRows(numBufferRows);
        query.setFields(fields);
        query.set("qt", "/select");
        query.setQuery("*:*");
		QueryResponse response;
		try {
			response = server.query(query);
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
		buffer = response.getResults();
		numFound = buffer.getNumFound();
		start += buffer.size();
		count -= buffer.size();
		pos = 0;
	}
	
	SolrDocument nextDocument() throws IOException {
		if (pos >= buffer.size()) {
			fetchNextDocumentChunk();
			
			if (pos >= buffer.size()) {
				return null;
			}
		}
		
		return buffer.get(pos++);
	}
	
	long getNumFound() {
		return numFound;
	}
}
