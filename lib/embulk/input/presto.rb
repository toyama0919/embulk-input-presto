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

        columns = []
        ExplainParser.parse(explain_result).each_with_index do |(name, type), i|
          columns << Column.new(i, name, TypeConverter.get_type(type))
        end
        columns
      end

      def init
        @client = Connection.get_client(task)
        @query = task["query"]
        @type_converter = TypeConverter.new

        Embulk.logger.info "SQL: #{@query}"
      end

      def run
        size = 0
        @client.query(@query) do |q|
          q.each_row {|row|
            converted_values = row.map.with_index { |value,i| @type_converter.convert_value(value, schema[i]) }
            page_builder.add(converted_values)
          }
          size = q.rows.size
        end

        page_builder.finish

        task_report = { size: size }
        return task_report
      end
    end
  end
end
