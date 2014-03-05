package com.chimpler.hive.solr;

import com.chimpler.hive.solr.ConfigurationUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class SolrStorageHandler implements HiveStorageHandler {
	private Configuration mConf = null;
	
	public SolrStorageHandler() {
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		ConfigurationUtil.copySolrProperties(properties, jobProperties);
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return SolrInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return new DummyMetaHook();
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return SolrOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return SolrSerDe.class;
	}

	@Override
	public Configuration getConf() {
		return this.mConf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.mConf = conf;
	}

	private class DummyMetaHook implements HiveMetaHook {

		@Override
		public void commitCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void commitDropTable(Table tbl, boolean deleteData)
				throws MetaException {
			boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
			if (deleteData && isExternal) {
				// nothing to do...
			} else if(deleteData && !isExternal) {
				String url = tbl.getParameters().get(ConfigurationUtil.URL);
	            HttpSolrServer server = new HttpSolrServer(url);
	            try {
					server.deleteByQuery("*:*");
					server.commit();
				} catch (SolrServerException e) {
					throw new MetaException(e.getMessage());
				} catch (IOException e) {
					throw new MetaException(e.getMessage());
				}
			}
		}

		@Override
		public void preCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void preDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

	}

	@Override
	public void configureInputJobProperties(TableDesc tableDescription, Map<String, String> jobProperties) {
		Properties properties = tableDescription.getProperties();
		ConfigurationUtil.copySolrProperties(properties, jobProperties);
	}

	@Override
	public void configureJobConf(TableDesc tableDescription, JobConf config) {
		Properties properties = tableDescription.getProperties();
		ConfigurationUtil.copySolrProperties(properties, config);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDescription, Map<String, String> jobProperties) {
		Properties properties = tableDescription.getProperties();
		ConfigurationUtil.copySolrProperties(properties, jobProperties);
	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		return new DefaultHiveAuthorizationProvider();
	}
}