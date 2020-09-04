require_relative 'packing_intervals/version'

module Sequel
  module Extensions
    module PackingIntervals
      def packing_intervals(partition:, dataset: self, start_date:, end_date:, cte_alias:)
        unless dataset.where(Sequel.lit('? > ?', start_date, end_date)).limit(1).all.empty?
          raise PackingIntervals, 'ERROR: dataset contain at least one record with start date after end date'
        end
        unless dataset.where(Sequel.lit('? = ?', start_date, end_date)).limit(1).all.empty?
          warn 'WARNING: dataset contain at least one record with start date = end date'
        end

        db = dataset.db

        missing_columns = partition - dataset.columns
        raise PackingIntervals::Error, "#{missing_columns} #{missing_columns.size == 1 ? 'is' : 'are'} not found in the dataset!" unless (missing_columns).empty?

        grpnm_partition = partition | [:grpnm]
        lag_end_date    = Sequel.function(:lag, end_date).over(:partition => partition, :order => [start_date, end_date])

        reduced = db["#{cte_alias}4".to_sym].
            with("#{cte_alias}1".to_sym, dataset.cross_apply(db["VALUES (1, #{start_date.to_s}), (-1, #{end_date.to_s})"].as(:a, [:type, :ts])).
                select(*partition, :ts, :type).
                select_append(
                    Sequel.case({-1 => nil}, Sequel.function(:row_number).over(:partition => [partition, :type].flatten, :order => start_date), :type).as(:s),
                    Sequel.case({1 => nil}, Sequel.function(:row_number).over(:partition => [partition, :type].flatten, :order => end_date), :type).as(:e))).
            with("#{cte_alias}2".to_sym, db["#{cte_alias}1".to_sym].select(*partition, :ts, :type, :s, :e).select_append { row_number.function.over(:partition => partition, :order => [:ts, type.desc]).as(:se) }).
            with("#{cte_alias}3".to_sym, db["#{cte_alias}2".to_sym].select_append(grpnm(partition: partition).as(:grpnm)).where(Sequel.lit('COALESCE(s - (se - s) - 1, (se - e) - e) = 0'))).
            with("#{cte_alias}4".to_sym, db["#{cte_alias}3".to_sym].select_group(*grpnm_partition).select_append { min(:ts).as(start_date) }.select_append { max(:ts).as(end_date) })

        # merge date intervals if no gap
        db["#{cte_alias}7".to_sym].
            with("#{cte_alias}5".to_sym, reduced.select_append(lag_end_date.as(:lag), Sequel.case({Sequel[start_date] <= Sequel.function(:dateadd, :day, 1, lag_end_date) => 0}, 1).as(:grp_start))).
            with("#{cte_alias}6".to_sym, db["#{cte_alias}5".to_sym].select_append(Sequel.function(:sum, :grp_start).over(partition: partition, order: [start_date, end_date]).as(:grp))).
            with("#{cte_alias}7".to_sym, db["#{cte_alias}6".to_sym].select_group(*(partition | [:grp])).select_append(
                Sequel.function(:min, start_date).as(start_date),
                Sequel.function(:max, end_date).as(end_date))).
            select(*partition, Sequel["#{cte_alias}7".to_sym][start_date], Sequel["#{cte_alias}7".to_sym][end_date])
      end

      private

      def grpnm(partition:)
        fn1 = Sequel.function(:row_number).over(:partition => partition, :order => :ts) - 1
        fn2 = Sequel.expr(fn1) / 2 + 1
        Sequel.function(:floor, fn2)
      end

      class Error < StandardError; end

    end
  end
  Sequel::Dataset.register_extension(:packing_intervals, Sequel::Extensions::PackingIntervals)
end
