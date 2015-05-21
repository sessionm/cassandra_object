class CreateTestTables < ActiveRecord::Migration
  def self.up
    create_table :users do |t|
      t.string :issue_id
      t.integer :position
    end
  end

  def self.down
    drop_table :users
  end
end
