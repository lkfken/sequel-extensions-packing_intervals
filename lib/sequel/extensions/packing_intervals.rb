require_relative 'packing_intervals/version'

module Sequel
  module Extensions
    module PackingIntervals
      def packing_intervals(partition:, dataset: self)
        missing_columns = partition - dataset.columns
        raise PackingIntervals::Error, "#{missing_columns} #{missing_columns.size == 1 ? 'is' : 'are'} not found in the dataset!" unless (missing_columns).empty?

        grpnm_partition = partition | [:grpnm]
        lag_end_date = Sequel.function(:lag, :end_date).over(:partition => partition, :order => [:start_date, :end_date])

        db = dataset.db
        reduced = db[:c4].
            with(:c1, dataset.cross_apply(DB['VALUES (1, START_DATE), (-1, END_DATE)'].as(:a, [:type, :ts])).
                select(*partition, :ts, :type).
                select_append(
                    Sequel.case({-1 => nil}, Sequel.function(:row_number).over(:partition => [partition, :type].flatten, :order => :start_date), :type).as(:s),
                    Sequel.case({1 => nil}, Sequel.function(:row_number).over(:partition => [partition, :type].flatten, :order => :end_date), :type).as(:e))).
            with(:c2, db[:c1].select(*partition, :ts, :type, :s, :e).select_append {row_number.function.over(:partition => partition, :order => [:ts, type.desc]).as(:se)}).
            with(:c3, db[:c2].select_append(grpnm(partition: partition).as(:grpnm)).where(Sequel.lit('COALESCE(s - (se - s) - 1, (se - e) - e) = 0'))).
            with(:c4, db[:c3].select_group(*grpnm_partition).select_append {min(:ts).as(:start_date)}.select_append {max(:ts).as(:end_date)})

        # merge date intervals if no gap
        db[:c7].
            with(:c5, reduced.select_append(lag_end_date.as(:lag), Sequel.case({Sequel[:start_date] <= Sequel.expr(lag_end_date + 1) => 0}, 1).as(:grp_start))).
            with(:c6, db[:c5].select_append(Sequel.function(:sum, :grp_start).over(partition: partition, order: [:start_date, :end_date]).as(:grp))).
            with(:c7, db[:c6].select_group(*(partition | [:grp])).select_append(
                Sequel.function(:min, :start_date).as(:start_date),
                Sequel.function(:max, :end_date).as(:end_date))).
            select(*partition, Sequel[:c7][:start_date], Sequel[:c7][:end_date])
      end

      private

      def grpnm(partition:)
        fn1 = Sequel.function(:row_number).over(:partition => partition, :order => :ts) - 1
        fn2 = Sequel.expr(fn1) / 2 + 1
        Sequel.function(:floor, fn2)
      end

      class Error < StandardError;
      end

    end
  end
  Sequel::Dataset.register_extension(:packing_intervals, Sequel::Extensions::PackingIntervals)
end
