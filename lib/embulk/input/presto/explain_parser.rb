module Embulk
  module Input
    class Presto < InputPlugin
      class ExplainParser
        def self.extract_text(explain_result)
          explain_result.flatten.last.lines.first
        end

        def self.build_output_columns(explain_result)
          columns_text = extract_text(explain_result)
          build_output_columns_proc(columns_text)
        end

        def self.build_output_columns_proc(columns_text)
          column_name_raw, column_type_raw = columns_text.split(' => ')
          names = column_name_raw.split('[').last.split(']').first.split(',').map{ |name| name.strip }
          types = column_type_raw.split('[').last.split(']').first.gsub(/\(.+?\)/, "").split(',').map{ |info| info.split(':').last }
          columns = []
          names.zip(types).each_with_index do |(name, type), i|
            columns << Column.new(i, name, TypeConverter.get_type(type))
          end
          columns
        end
      end
    end
  end
end
