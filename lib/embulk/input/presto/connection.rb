module Embulk
  module Input
    class Presto < InputPlugin
      class Connection
        def self.get_client(task)
          ::Presto::Client.new(
            server: "#{task['host']}:#{task['port']}",
            catalog: task['catalog'],
            user: task['user'],
            schema: task['schema']
          )
        end

        def initialize(task)
          @presto_client = self.class.get_client(task)
          @query = task['query']

          Embulk.logger.info "SQL: #{@query}"
        end

        def query
          @presto_client.query(@query) do |q|
            q.each_row {|row|
              yield(row) if block_given?
            }
          end
        end
      end
    end
  end
end
