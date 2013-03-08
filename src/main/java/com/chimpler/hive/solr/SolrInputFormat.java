package com.chimpler.hive.solr;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SolrInputFormat extends
                HiveInputFormat<LongWritable, MapWritable> {
    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(
                    InputSplit split, JobConf conf, Reporter reporter)
                    throws IOException {
            List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(conf);

            boolean addAll = (readColIDs.size() == 0);

            String columnString = conf.get(ConfigurationUtil.COLUMN_MAPPING);
            if (StringUtils.isBlank(columnString)) {
                    throw new IOException("no column mapping found!");
            }

            String[] columns = ConfigurationUtil.getAllColumns(columnString);
            if (readColIDs.size() > columns.length) {
                    throw new IOException(
                                    "read column count larger than that in column mapping string!");
            }

            String[] cols;
            if (addAll) {
                    cols = columns;
            } else {
                    cols = new String[readColIDs.size()];
                    for(int i = 0; i < cols.length; i++){
                            cols[i] = columns[readColIDs.get(i)];
                    }
            }
            String filterExprSerialized =
                    conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);

                  if (filterExprSerialized != null){
                      ExprNodeDesc filterExpr =
                          Utilities.deserializeExpression(filterExprSerialized, conf);
                      /*String columnNameProperty = conf.get(
                                  org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS);
                      System.err.println("======list columns:" + columnNameProperty);*/
                      dumpFilterExpr(filterExpr);
                      //TODO:
                  }

                  return new SolrReader(ConfigurationUtil.getUrl(conf),
                    				(SolrSplit) split, cols, ConfigurationUtil.getNumInputBufferRows(conf));
            }

            @Override
            public InputSplit[] getSplits(JobConf conf, int numSplits)
                            throws IOException {
                    return SolrSplit.getSplits(conf, ConfigurationUtil.getUrl(conf), numSplits);
            }


            void dumpFilterExpr(ExprNodeDesc expr){
                    if(expr == null) return;
                    List<ExprNodeDesc> children = expr.getChildren();
                    if(children != null && children.size() > 0){
                            for(ExprNodeDesc e : children){
                                    dumpFilterExpr(e);
                            }
                    }
            }
}