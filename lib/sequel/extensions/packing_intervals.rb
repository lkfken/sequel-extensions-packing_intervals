# see https://www.red-gate.com/simple-talk/sql/t-sql-programming/calculating-gaps-between-overlapping-time-intervals-in-sql/

require_relative 'packing_intervals/version'

module Sequel
  module Extensions
    module PackingIntervals
      def packing_intervals(partition: nil, dataset: self, start_date: :start_date, end_date: :end_date, cte_alias: :cte)

        unless dataset.where(Sequel.lit('? > ?', start_date, end_date)).limit(1).all.empty?
          raise PackingIntervals::Error, 'ERROR: dataset contain at least one record with start date after end date'
        end

        unless dataset.where(Sequel.lit('? = ?', start_date, end_date)).limit(1).all.empty?
          warn 'WARNING: dataset contain at least one record with start date = end date'
        end

        partition       ||= dataset.columns - [start_date, end_date]
        grpnm_partition = partition | [:grpnm]

        db = dataset.db

        missing_columns = partition - dataset.columns
        raise PackingIntervals::Error, "#{missing_columns} #{missing_columns.size == 1 ? 'is' : 'are'} not found in the dataset!" unless (missing_columns).empty?

        reduced = db["#{cte_alias}4".to_sym].
            # add time stamp (ts) and date type indicator (type, 1 = start date, -1 = end date)
            with("#{cte_alias}1".to_sym, dataset.cross_apply(db["VALUES (1, #{start_date.to_s}), (-1, #{end_date.to_s})"].as(:a, [:type, :ts])).
                # select(*partition, :ts, :type).
                # append the start date sequence number and end date sequence number
                select_append(start_date_seqnm(partition: partition).as(:s), end_date_seqnm(partition: partition).as(:e))).
            with("#{cte_alias}2".to_sym, db["#{cte_alias}1".to_sym].
                # select(*partition, :ts, :type, :s, :e).
                # append the sequence number of partition
                select_append { row_number.function.over(:partition => partition, :order => [:ts, type.desc]).as(:se) }).
            with("#{cte_alias}3".to_sym, db["#{cte_alias}2".to_sym].
                # group rows by pair(2) and filter/choose those rows where (start seq num - (seq num - start seq num) - 1) = 0
                # or where ((seq num - end seq num) - end seq num) = 0
                # the COALESCE function is to eliminates overlaps
                # @grpnm is the group number (or island number)
                select_append(grpnm(partition: partition).as(:grpnm)).where(Sequel.lit('COALESCE(s - (se - s) - 1, (se - e) - e) = 0'))).
            with("#{cte_alias}4".to_sym, db["#{cte_alias}3".to_sym].
                # for each island, get the minimum start date and maximum end date
                select_group(*grpnm_partition).select_append { min(:ts).as(start_date) }.select_append { max(:ts).as(end_date) })

        # merge date intervals if no gap
        lag_fn = Sequel.function(:lag, end_date).over(:partition => partition, :order => [start_date, end_date])
        db["#{cte_alias}8".to_sym].
            with("#{cte_alias}5".to_sym, reduced.select_append(lag_fn.as(:lag))).
            with("#{cte_alias}6".to_sym, db["#{cte_alias}5".to_sym].select_append(
                Sequel.case({Sequel[start_date] <= Sequel.function(:dateadd, :day, 1, :lag) => 0}, 1).as(:grp_start))).
            with("#{cte_alias}7".to_sym, db["#{cte_alias}6".to_sym].select_append(Sequel.function(:sum, :grp_start).over(partition: partition, order: [start_date, end_date]).as(:grp))).
            with("#{cte_alias}8".to_sym, db["#{cte_alias}7".to_sym].select_group(*(partition | [:grp])).select_append(
                Sequel.function(:min, start_date).as(start_date),
                Sequel.function(:max, end_date).as(end_date))).
            select(*partition, Sequel["#{cte_alias}8".to_sym][start_date], Sequel["#{cte_alias}8".to_sym][end_date])
      end

      private

      # @param type date type (1 = start date, -1 = end date)
      # if type is an end date, then return nil
      # if type is a start date, then return a row number based on the ts (timestamp) order
      def start_date_seqnm(partition:, order: :ts, type: :type)
        Sequel.case({-1 => nil}, Sequel.function(:row_number).over(:partition => [partition, type].flatten, :order => order), type)
      end

      # @param type date type (1 = start date, -1 = end date)
      # if type is a start date, then return nil
      # if type is an end date, then return a row number based on the ts (timestamp) order
      def end_date_seqnm(partition:, order: :ts, type: :type)
        Sequel.case({1 => nil}, Sequel.function(:row_number).over(:partition => [partition, type].flatten, :order => order), type)
      end

      # created a grouping column as (ROW_NUMBER()+1)/2, which is a well-known trick for pairing consecutive rows
      def grpnm(partition:, order: :ts)
        fn1 = Sequel.function(:row_number).over(:partition => partition, :order => order) - 1
        fn2 = Sequel.expr(fn1) / 2 + 1
        Sequel.function(:floor, fn2)
      end

      class Error < StandardError; end

    end
  end
  Sequel::Dataset.register_extension(:packing_intervals, Sequel::Extensions::PackingIntervals)
end
