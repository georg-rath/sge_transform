# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sge_transform/version'

Gem::Specification.new do |spec|
  spec.name          = "sge_transform"
  spec.version       = SgeTransform::VERSION
  spec.authors       = ["Georg Rath"]
  spec.email         = ["rath.georg@gmail.com"]
  spec.summary       = "Transform Sun GridEngine accounting file data to RDBMS format."
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "sequel", "> 4.0"
  spec.add_dependency "sqlite3", "> 1.3"
  spec.add_dependency "pg", "~> 0.13"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.1"
end
