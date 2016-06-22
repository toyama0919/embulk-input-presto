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
          "columns" => config.param("columns", :array)
        }

        columns = task['columns'].each_with_index.map do |c, i|
          Column.new(i, c["name"], c["type"].to_sym)
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def init
        @client = ::Presto::Client.new(
          server: "#{task['host']}:#{task['port']}",
          catalog: task['catalog'],
          user: task['user'],
          schema: task['schema']
        )
        @query = task["query"]

        Embulk.logger.info "SQL: #{@query}"
      end

      def run
        size = 0
        @client.query(@query) do |q|
          q.each_row {|row|
            converted_values = row.map.with_index { |value,i| convert_value(value, schema[i]) }
            page_builder.add(converted_values)
          }
          size = q.rows.size
        end

        page_builder.finish

        task_report = { size: size }
        return task_report
      end

      def convert_value(value, field)
        return nil if value.nil?
        case field["type"]
        when :string
          value
        when :long
          value.to_i
        when :double
          value.to_f
        when :boolean
          if value.is_a?(TrueClass) || value.is_a?(FalseClass)
            value
          else
            downcased_val = value.downcase
            case downcased_val
            when 'true' then true
            when 'false' then false
            when '1' then true
            when '0' then false
            else nil
            end
          end
        when :timestamp
          Time.parse(value)
        when :json
          value
        else
          raise "Unsupported type #{field['type']}"
        end
      end
    end
  end
end
