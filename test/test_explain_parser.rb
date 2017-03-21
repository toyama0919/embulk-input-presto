require_relative './helper'
require 'embulk/input/presto'
require 'yaml'

Presto = Embulk::Input::Presto

module Embulk
  class Input::Presto
    class TestExplainParser < Test::Unit::TestCase
      def startup
      end

      def shutdown
      end

      sub_test_case "build_output_columns_proc" do
        def test_normal
          columns_text = "- Output[keyword, count] => [trim:varchar, count_1:bigint]\n"
          assert_equal(
            ExplainParser.build_output_columns_proc(columns_text),
            [
              Column.new(0, "keyword", :string),
              Column.new(1, "count", :long)
            ]
          )
        end

        def test_json
          columns_text = "- Output[customer_id, session_id, log_time, log_date, keywords] => [customer_id:integer, session_id:varchar(256), log_time:timestamp, log_date:date, split:array(varchar(256))]\n"
          assert_equal(
            ExplainParser.build_output_columns_proc(columns_text),
            [
              Column.new(0, "customer_id", :long),
              Column.new(1, "session_id", :string),
              Column.new(2, "log_time", :timestamp),
              Column.new(3, "log_date", :timestamp),
              Column.new(4, "keywords", :json)
            ]
          )
        end

        def test_hive
          columns_text = "- Output[data, user_id] => [data:map(varchar, varchar), user_id:bigint]\n"
          assert_equal(
            ExplainParser.build_output_columns_proc(columns_text),
            [
              Column.new(0, "data", :json),
              Column.new(1, "user_id", :long)
            ]
          )
        end
      end
    end
  end
end