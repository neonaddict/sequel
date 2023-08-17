SEQUEL_ADAPTER_TEST = :postgres
ENV['SEQUEL_POSTGRES_URL']="postgresql://postgres:password@localhost:5432/postgres"

require_relative 'spec_helper'
require 'benchmark'

uses_pg = Sequel::Postgres::USES_PG if DB.adapter_scheme == :postgres
uses_pg_or_jdbc = uses_pg || DB.adapter_scheme == :jdbc

Sequel.extension :pg_extended_date_support
DB.extension :pg_array, :pg_range, :pg_row, :pg_inet, :pg_json, :pg_enum
begin
  DB.extension :pg_interval
rescue LoadError
end
DB.extension :pg_hstore if DB.type_supported?('hstore')
DB.extension :pg_multirange if DB.server_version >= 140000

if uses_pg && ENV['SEQUEL_PG_AUTO_PARAMETERIZE']
  if ENV['SEQUEL_PG_AUTO_PARAMETERIZE'] = 'in_array_string'
    DB.extension :pg_auto_parameterize_in_array
    DB.opts[:treat_string_list_as_text_array] = 't'
  elsif ENV['SEQUEL_PG_AUTO_PARAMETERIZE'] = 'in_array'
    DB.extension :pg_auto_parameterize_in_array
  else
    DB.extension :pg_auto_parameterize
  end
end

describe 'PostgreSQL adapter' do
  before do
    @db = DB
    @db.disconnect
  end
  after do
    @db.disconnect
  end

  it {
    connection = @db

    connection.drop_table(:organizations, cascade: true) if connection.table_exists?(:organizations)
    connection.drop_table(:users, cascade: true) if connection.table_exists?(:users)
    connection.drop_table(:user_passports, cascade: true) if connection.table_exists?(:user_passports)
    connection.drop_table(:books, cascade: true) if connection.table_exists?(:books)
    connection.drop_table(:movies, cascade: true) if connection.table_exists?(:movies)
    connection.drop_table(:videogames, cascade: true) if connection.table_exists?(:videogames)
    connection.drop_table(:hobbies, cascade: true) if connection.table_exists?(:hobbies)
    connection.drop_table(:vinyls, cascade: true) if connection.table_exists?(:vinyls)
    connection.drop_table(:pets, cascade: true) if connection.table_exists?(:pets)
    connection.drop_table(:skills, cascade: true) if connection.table_exists?(:skills)
    connection.drop_table(:dreams, cascade: true) if connection.table_exists?(:dreams)

    connection.create_table :organizations do
      primary_key :id

      column  :name, 'varchar(256)'
    end

    connection.create_table :users do
      primary_key :id

      column  :name, 'varchar(256)'
      column  :age, :integer
      foreign_key :organization_id, :organizations, null: false, on_delete: :cascade
    end

    connection.create_table :user_passports do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :info, 'varchar(256)'
    end

    connection.create_table :books do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :title, 'varchar(256)'
    end

    connection.create_table :movies do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :title, 'varchar(256)'
    end

    connection.create_table :videogames do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :title, 'varchar(256)'
    end

    connection.create_table :hobbies do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :title, 'varchar(256)'
    end

    connection.create_table :vinyls do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :title, 'varchar(256)'
    end

    connection.create_table :pets do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :name, 'varchar(256)'
    end

    connection.create_table :skills do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :title, 'varchar(256)'
    end

    connection.create_table :dreams do
      primary_key :id

      foreign_key :user_id, :users, null: false, on_delete: :cascade
      column  :description, 'varchar(256)'
    end

    organizations = connection[:organizations]
    users = connection[:users]
    books = connection[:books]
    movies = connection[:movies]
    videogames = connection[:videogames]
    hobbies = connection[:hobbies]
    vinyls = connection[:vinyls]
    pets = connection[:pets]
    skills = connection[:skills]
    dreams = connection[:dreams]

    organizations.insert(name: "Test Org")

    org = organizations.first

    NUM_OF_USERS = 50
    ASSOC_COUNT = 100

    NUM_OF_USERS.times do |i|
      users.insert(
        name: "Name #{i}",
        age: rand(18..50),
        organization_id: org[:id]
      )
    end

    users.select_map(:id).each do |user_id|
      ASSOC_COUNT.times do |i|
        books.insert(
          title: "Book #{i}",
          user_id: user_id
        )

        movies.insert(
          user_id: user_id,
          title: "Title #{i}"
        )

        videogames.insert(
          user_id: user_id,
          title: "Title #{i}"
        )

        hobbies.insert(
          user_id: user_id,
          title: "Title #{i}"
        )

        vinyls.insert(
          user_id: user_id,
          title: "Title #{i}"
        )

        pets.insert(
          user_id: user_id,
          name: "Name #{i}"
        )

        skills.insert(
          user_id: user_id,
          title: "Title #{i}"
        )

        dreams.insert(
          user_id: user_id,
          description: "Description #{i}"
        )
      end
    end

    Benchmark.bm do |x|
      x.report("in threads") do
        threads = []

        threads << Thread.new do
          x.report("inside thread") do
            books.all
          end
        end

        threads << Thread.new do
          movies.all
        end

        threads << Thread.new do
          videogames.all
        end

        threads << Thread.new do
          hobbies.all
        end

        threads << Thread.new do
          vinyls.all
        end

        threads << Thread.new do
          pets.all
        end

        threads << Thread.new do
          skills.all
        end

        threads << Thread.new do
          dreams.all
        end

        threads.each(&:join)
      end

      x.report("in processes") do
        pids = []

        pids << Process.fork do
          books.db.disconnect
          books.all
        end

        pids << Process.fork do
          movies.db.disconnect
          movies.all
        end

        pids << Process.fork do
          videogames.db.disconnect
          videogames.all
        end

        pids << Process.fork do
          hobbies.db.disconnect
          hobbies.all
        end

        pids << Process.fork do
          vinyls.db.disconnect
          vinyls.all
        end

        pids << Process.fork do
          pets.db.disconnect
          pets.all
        end

        pids << Process.fork do
          skills.db.disconnect
          skills.all
        end

        pids << Process.fork do
          dreams.db.disconnect
          dreams.all
        end

        Process.waitall
      end
    end
  }
end