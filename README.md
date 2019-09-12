# Sequel::Extensions::PackingIntervals

Packing date intervals

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sequel-extensions-packing_intervals'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sequel-extensions-packing_intervals

## Usage
```ruby
...
require 'sequel/extensions/packing_intervals'
...
dataset = DB[:a].dataset.select(:user_id, :from_dt___start_date, :to_dt___end_date).from_self  # dataset is expected to have [start_date] and [end_date]
dataset = dataset.extension(:packing_intervals)
dataset.packing_intervals(:partition => [:user_id])

```


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/lkfken/sequel-extension-packing_intervals.
