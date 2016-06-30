module Embulk
  module Input
    class Presto < InputPlugin
      class ExplainParser
        def self.parse(explain_result)
          explain_text = explain_result.flatten.last.lines.first
          column_name_raw, column_type_raw = explain_text.split(' => ')
          names = column_name_raw.split('[').last.split(']').first.split(',').map{ |name| name.strip }
          types = column_type_raw.split('[').last.split(']').first.split(',').map{ |info| info.split(':').last }
          Hash[*names.zip(types).flatten]
        end
      end
    end
  end
end
