
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "sequel/extensions/packing_intervals/version"

Gem::Specification.new do |spec|
  spec.name          = "sequel-extensions-packing_intervals"
  spec.version       = Sequel::Extensions::PackingIntervals::VERSION
  spec.authors       = ["Kenneth Leung"]
  spec.email         = ["kenneth@leungs.us"]

  spec.summary       = %q{This extension provide a method that would return merged date range records.}
  spec.description   = %q{This extension provide a method that would return merged date range records.}
  spec.homepage      = "https://github.com/lkfken/sequel-extensions-packing_intervals"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "https://github.com/lkfken/sequel-extensions-packing_intervals"

    spec.metadata["homepage_uri"] = spec.homepage
    spec.metadata["source_code_uri"] = "https://github.com/lkfken/sequel-extensions-packing_intervals"
    spec.metadata["changelog_uri"] = "https://github.com/lkfken/sequel-extensions-packing_intervals.."
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
end
