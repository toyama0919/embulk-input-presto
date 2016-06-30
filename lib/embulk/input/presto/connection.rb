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
      end
    end
  end
end
