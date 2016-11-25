module Embulk
  module Input
    class Presto < InputPlugin
      class TypeConverter

        def initialize
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
            elsif value.class == Fixnum
              value == 0 ? false : true
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

        def self.get_type(type)
          if type.start_with?("boolean")
            :boolean
          elsif type.start_with?("tinyint")
            :boolean
          elsif type.start_with?("bigint")
            :long
          elsif type.start_with?("integer")
            :long
          elsif type.start_with?("double")
            :double
          elsif type.start_with?("decimal")
            :double
          elsif type.start_with?("varchar")
            :string
          elsif type.start_with?("varbinary")
            :string
          elsif type.start_with?("json")
            :json
          elsif type.start_with?("date")
            :timestamp
          elsif type.start_with?("time")
            :timestamp
          elsif type.start_with?("time with time zone")
            :timestamp
          elsif type.start_with?("timestamp")
            :timestamp
          elsif type.start_with?("timestamp with time zone")
            :timestamp
          elsif type.start_with?("interval year to month")
            :timestamp
          elsif type.start_with?("interval day to second")
            :timestamp
          elsif type.start_with?("array")
            :json
          elsif type.start_with?("map")
            :json
          elsif type.start_with?("row")
            :json
          end
        end
      end
    end
  end
end
