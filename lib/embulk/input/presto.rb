module Embulk
  class InputPresto < InputPlugin
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
        "columns" => config.param("columns", :array, default: [])
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
      @client = Presto::Client.new(
        server: "#{task['host']}:#{task['port']}",
        catalog: task['catalog'],
        user: task['user'],
        schema: task['schema']
      )
      @query = task["query"]
    end

    def run
      @client.query(@query) do |q|
        q.each_row {|row|
          page_builder.add(row)
        }
      end

      page_builder.finish

      task_report = {}
      return task_report
    end
  end
end
