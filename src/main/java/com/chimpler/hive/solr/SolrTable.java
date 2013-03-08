package com.chimpler.hive.solr;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

public class SolrTable {
	private static final int MAX_INPUT_BUFFER_ROWS = 100000;
	private static final int MAX_OUTPUT_BUFFER_ROWS = 100000;
	private HttpSolrServer server;
	private String url;
	private Collection<SolrInputDocument> outputBuffer;
	private int numInputBufferRows = MAX_INPUT_BUFFER_ROWS;
	private int numOutputBufferRows = MAX_OUTPUT_BUFFER_ROWS;	

	public SolrTable(String url) {
        this.server = new HttpSolrServer(url);
        this.url = url;
        this.outputBuffer = new ArrayList<SolrInputDocument>(numOutputBufferRows);
	}

	public void save(SolrInputDocument doc) throws IOException {
		outputBuffer.add(doc);

		if (outputBuffer.size() >= numOutputBufferRows) {
			flush();
		}
	}
	
	public void flush() throws IOException {
		try {
			if (!outputBuffer.isEmpty()) {
				server.add(outputBuffer);
				outputBuffer.clear();
			}
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public long count() throws IOException {
		return findAll(new String[0], 0, 0).getNumFound();
	}

	public SolrTableCursor findAll(String[] fields, int start, int count) throws IOException {
		return new SolrTableCursor(url, fields, start, count, numInputBufferRows);
	}
	
	public void drop() throws IOException{
		try {
			server.deleteByQuery("*:*");
			server.commit();
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public void commit() throws IOException {
		try {
			flush();
			server.commit();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public void rollback() throws IOException {
		try {
			outputBuffer.clear();
			server.rollback();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public int getNumOutputBufferRows() {
		return numOutputBufferRows;
	}

	public void setNumOutputBufferRows(int numOutputBufferRows) {
		this.numOutputBufferRows = numOutputBufferRows;
	}

	public int getNumInputBufferRows() {
		return numInputBufferRows;
	}

	public void setNumInputBufferRows(int numInputBufferRows) {
		this.numOutputBufferRows = numInputBufferRows;
	}
	
}