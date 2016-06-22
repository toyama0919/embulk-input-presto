require_relative './helper'
require 'embulk/input/presto'
require 'yaml'

Presto = Embulk::Input::Presto

module Embulk
  class Input::Presto
    class TestTransaction < Test::Unit::TestCase
      def control
        Proc.new {|task| task_reports = [] }
      end

      sub_test_case "transaction" do
        def test_normal
          yaml = YAML.load(%(
            host: presto01
            catalog: store
            schema: public
            query: |
              SELECT
                trim(upper(url_decode(keyword))) AS keyword,
                count(*) as count
              FROM search
              CROSS JOIN UNNEST(split(keywords, ',')) AS t (keyword)
              WHERE log_date >= (CURRENT_DATE - INTERVAL '90' DAY)
               AND length(keywords) != 256
              group by keyword
              having count(*) >= 10
              order by count(*) desc
            columns:
              - {name: keyword, type: string}
              - {name: count, type: long}
            )
          )
          config = DataSource.new(yaml)
          Presto.transaction(config, &control)
        end

        def test_minimum
          yaml = YAML.load(%(
            query: |
              SELECT
                trim(upper(url_decode(keyword))) AS keyword,
                count(*) as count
              FROM search
              CROSS JOIN UNNEST(split(keywords, ',')) AS t (keyword)
              WHERE log_date >= (CURRENT_DATE - INTERVAL '90' DAY)
               AND length(keywords) != 256
              group by keyword
              having count(*) >= 10
              order by count(*) desc
            columns:
              - {name: keyword, type: string}
              - {name: count, type: long}

            )
          )
          config = DataSource.new(yaml)
          Presto.transaction(config, &control)
        end
      end
    end
  end
end