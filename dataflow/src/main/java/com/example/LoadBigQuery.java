/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.example.common.DataflowExampleUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.ValueProvider;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class LoadBigQuery {
	static class FormatAsTableRowFn extends DoFn<String[], TableRow> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow().set("word", c.element()[0]).set("count", c.element()[1]);
			c.output(row);
		}
	}

	static class ExtractColumnFn extends DoFn<String, String[]> {
		private final Aggregator<Long, Long> emptyLines = createAggregator("emptyLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			if (c.element().trim().isEmpty()) {
				emptyLines.addValue(1L);
			}

			String[] columns = c.element().split(",");

			if (columns.length != 0) {
				c.output(columns);
			}
		}
	}

	static class CleansingFn extends DoFn<String[], String[]> {

		@Override
		public void processElement(ProcessContext c) {

			String[] columns = c.element();
			TableSchema schema = getSchema();

			for (int i = 0; i < columns.length; i++) {
				String type = schema.getFields().get(i).getType();

				try {
					validateColumn(columns[i], type);
				} catch (Exception e) {
					columns[i] = "0";
				}
			}
			c.output(columns);
		}

		public void validateColumn(String column, String type) throws NumberFormatException {
			switch (type) {
			case "INTEGER":
				Integer.parseInt(column);
				break;
			default:
				break;
			}
		}
	}

	public static class DataCleansing extends PTransform<PCollection<String>, PCollection<String[]>> {
		@Override
		public PCollection<String[]> apply(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtractColumnFn())).apply(ParDo.of(new CleansingFn()));
		}
	}

	private static TableSchema getSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("word").setType("STRING"));
		fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	private static TableReference getTableReference(Options options) {
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(options.getBigQueryTable());
		return tableRef;
	}

	public interface Options extends PipelineOptions, DataflowExampleUtils.DataflowExampleUtilsOptions {
		@Description("Path of the file to read from")
		ValueProvider<String> getInputFile();

		void setInputFile(ValueProvider<String> value);

		@Description("Path of the file to write to")
		ValueProvider<String> getOutput();

		void setOutput(ValueProvider<String> value);
	}

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setBigQuerySchema(getSchema());
		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile())).apply(new DataCleansing())
				.apply(ParDo.of(new FormatAsTableRowFn()))
				.apply(BigQueryIO.Write.to(getTableReference(options)).withSchema(getSchema()));

		p.run();
	}
}
