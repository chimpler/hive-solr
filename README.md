Installation
============

To install:

	$ git clone http://github.org/chimpler/hive-solr
	$ cd hive-solr
	$ mvn package
	$ cp target/hive-solr-0.0.1-SNAPSHOT-jar-with-dependencies.jar `$HIVE_HOME/lib`

Usage
=====

If your SOLR schema is something like:

	<?xml version="1.0" ?>
	<schema name="segment_overlap" version="1.1">
	  <types>
	   <fieldtype name="string" class="solr.StrField" sortMissingLast="true" omitNorms="true"/>
	   <fieldType name="int" class="solr.TrieIntField" precisionStep="0" positionIncrementGap="0"/>
	   <fieldType name="float" class="solr.TrieFloatField" precisionStep="0" positionIncrementGap="0"/>
	   <fieldType name="long" class="solr.TrieLongField" precisionStep="0" positionIncrementGap="0"/>
	   <fieldType name="double" class="solr.TrieDoubleField" precisionStep="0" positionIncrementGap="0"/>
	  </types>

	 <fields>
	   <field name="id" type="int" indexed="true" stored="true" required="true" />
	   <field name="_version_" type="long" indexed="true" stored="true" required="true" />
	   <field name="item_id" type="int" indexed="true" stored="true" required="true" />
	   <field name="name" type="string" indexed="true" stored="true" required="true" />
	   <field name="year" type="int" indexed="true" stored="true" required="true" />
	   <field name="month" type="int" indexed="true" stored="true" required="true" />
	   <field name="shipping_method" type="int" indexed="true" stored="true" required="true" />
	   <field name="us_sold" type="int" indexed="true" stored="true" required="true" />
	   <field name="ca_sold" type="int" indexed="true" stored="true" required="true" />
	   <field name="fr_sold" type="int" indexed="true" stored="true" required="true" />
	   <field name="uk_sold" type="int" indexed="true" stored="true" required="true" />
	 </fields>

	 <!-- field to use to determine and enforce document uniqueness. -->
	 <uniqueKey>id</uniqueKey>

	 <!-- field for the QueryParser to use when an explicit fieldname is absent -->
	 <defaultSearchField>name</defaultSearchField>

	 <!-- SolrQueryParser configuration: defaultOperator="AND|OR" -->
	 <solrQueryParser defaultOperator="AND"/>
	</schema>

You can create an external table as follows:

	hive> create external table solr_items2 (
	    id INT,
	    item_id INT,
	    name STRING,
	    year INT,
	    month INT,
	    shipping_method INT,
	    us_sold INT,
	    ca_sold INT,
	    fr_sold INT,
	    uk_sold INT
	) stored by "com.chimpler.hive.solr.SolrStorageHandler"
	with serdeproperties ("solr.column.mapping"="id,item_id,name,year,month,shipping_method,us_sold,ca_sold,fr_sold,uk_sold")
	tblproperties ("solr.url" = "http://localhost:8983/solr/core0","solr.buffer.input.rows"="10000","solr.buffer.output.rows"="10000");

Note that solr.buffer.input.rows and solr.buffer.output.rows are optional (default is 100000).

Acknowledgements
================

Thank you to yc-huang for his work on hive-mongo (https://github.com/yc-huang/Hive-mongo) that served as a base for hive-solr.
