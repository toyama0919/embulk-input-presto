require_relative 'presto/type_converter'
require_relative 'presto/explain_parser'
require_relative 'presto/connection'

module Embulk
  module Input
    class Presto < InputPlugin
      require 'presto-client'

      Plugin.register_input("presto", self)

      def self.transaction(config, &control)
        task = {
          "host" => config.param("host", :string, default: "localhost"),
          "port" => config.param("port", :integer, default: 8080),
          "schema" => config.param("schema", :string, default: "default"),
          "catalog" => config.param("catalog", :string, default: "native"),
          "query" => config.param("query", :string),
          "user" => config.param("user", :string, default: "embulk"),
          "columns" => config.param("columns", :array, default: nil)
        }

        columns = if task['columns']
          task['columns'].each_with_index.map do |c, i|
            Column.new(i, c["name"], c["type"].to_sym)
          end
        else
          build_output_columns(task)
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.build_output_columns(task)
        explain_query = "explain (FORMAT TEXT) " + task["query"]
        Embulk.logger.debug("SQL: #{explain_query}")
        explain_result = Connection.get_client(task).run("explain (FORMAT TEXT) " + task["query"])

        ExplainParser.build_output_columns(explain_result)
      end

      def init
        @client = Connection.new(task)
        @type_converter = TypeConverter.new
      end

      def run
        size = @client.query do |row|
          converted_values = row.map.with_index { |value,i| @type_converter.convert_value(value, schema[i]) }
          page_builder.add(converted_values)
        end

        page_builder.finish

        task_report = {}
        return task_report
      end
    end
  end
end
