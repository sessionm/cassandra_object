class CreateTestTables < ActiveRecord::Migration
  def self.up
    create_table :users do |t|
      t.string :issue_id
      t.integer :position
    end

    create_table :roles do |t|
      t.string :name
      t.integer :user_id
    end
  end

  def self.down
    drop_table :users
    drop_table :roles
  end
end
