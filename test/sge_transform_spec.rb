require 'rspec'
require 'sge_transform'
require 'sequel'

describe 'module SgeTransform' do
  accounting_file = 'test/qacc_test'
  db_file = 'test/testdb'

  it 'should transform cleanly' do
    SgeTransform::Transformer.new(accounting_file, "sqlite://#{db_file}", 500)
    SgeTransform.transform

    db = Sequel.connect("sqlite://#{db_file}")
    expect(db[:jobs].count).to eq(100)

    File.delete(db_file)
    File.delete('sge_transform.state')
  end


end
