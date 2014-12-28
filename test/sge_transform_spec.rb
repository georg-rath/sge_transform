require 'rspec'
require 'sge_transform'
require 'sequel'

describe 'module SgeTransform' do
  accounting_file = 'test/qacc_test'
  db_file = 'test/testdb'

  it 'should transform cleanly' do
    SgeTransform.transform(accounting_file, "sqlite://#{db_file}")

    db = Sequel.connect("sqlite://#{db_file}")
    expect(db[:jobs].count).to eq(100)

    File.delete(db_file)
  end


end