# async sequel extensions, for use with em-mysql
#
#   require 'em/mysql'
#   DB = Sequel.connect(:adapter => 'mysql', :user => 'root', :database => 'test', ...)
#   EventedMysql.settings.update(..., :on_error => proc{|e| log 'error', e })
#
#   def log *args
#     p [Time.now, *args]
#   end
#
#   DB[:table].where(:id < 100).async_update(:field => 'new value') do |num_updated|
#     log "done updating #{num_updated} rows"
#   end
#
#   DB[:table].async_insert(:field => 'value') do |insert_id|
#     log "inserted row #{insert_id}"
#   end
#
#   DB[:table].async_multi_insert([:field], [ ['one'], ['two'], ['three'] ]) do
#     log "done inserting 3 rows"
#   end
#
#   DB[:table].limit(10).async_each do |row|
#     log "got a row", row
#   end; log "this will be printed before the query returns"
#
#   DB[:table].async_all do |rows|
#     DB[:table].async_multi_insert([:field], rows.map{|r| "new_#{r[:field]}" })
#   end
#
#   DB[:table].async_all do |rows|
#     num = rows.size
#
#     rows.each{ |r|
#       DB[:table].where(:id => r[:id]).async_update(:field => rand(10000).to_s) do
#         num = num-1
#         if num == 0
#           log "last update completed"
#         end
#       end
#     }
#   end
#
#   DB[:table].async_count do |num_rows|
#     log "table has #{num_rows} rows"
#   end

module Sequel
  class Dataset
    def async_insert *args, &cb
      EventedMysql.insert insert_sql(*args), &cb
      nil
    end

    def async_insert_ignore *args, &cb
      EventedMysql.insert insert_sql(*args).sub(/insert/i, 'INSERT IGNORE'), &cb
      nil
    end

    def async_update *args, &cb
      EventedMysql.update update_sql(*args), &cb
      nil
    end

    def async_delete &cb
      EventedMysql.execute delete_sql, &cb
      nil
    end

    def async_multi_insert *args, &cb
      EventedMysql.execute multi_insert_sql(*args).first, &cb
      nil
    end

    def async_multi_insert_ignore *args, &cb
      EventedMysql.execute multi_insert_sql(*args).first.sub(/insert/i, "INSERT IGNORE"), &cb
      nil
    end

    def async_each
      EventedMysql.select(select_sql) do |rows|
        rows.each{|r|
          if row_proc = @row_proc
            yield row_proc.call(r)
          else
            yield r
          end
        }
      end
      nil
    end

    def async_all
      EventedMysql.select(sql) do |rows|
        if row_proc = @row_proc
          yield(rows.map{|r| row_proc.call(r) })
        else
          yield(rows)
        end
      end
      nil
    end

    def async_count &cb
      if options_overlap(COUNT_FROM_SELF_OPTS)
        from_self.async_count(&cb)
      else
        clone(STOCK_COUNT_OPTS).async_each{|r|
          yield r.is_a?(Hash) ? r.values.first.to_i : r.values.values.first.to_i
        }
      end
      nil
    end
  end

  class Model
    def async_update *args, &cb
      this.async_update(*args, &cb)
      set(*args)
      self
    end

    def async_delete &cb
      this.async_delete(&cb)
      nil
    end

    class << self
      [ :async_insert,
        :async_insert_ignore,
        :async_multi_insert,
        :async_multi_insert_ignore,
        :async_each,
        :async_all,
        :async_update,
        :async_count ].each do |method|
        class_eval %[
          def #{method} *args, &cb
            dataset.#{method}(*args, &cb)
          end
        ]
      end

      # async version of Model#[]
      def async_lookup args
        unless Hash === args
          args = primary_key_hash(args)
        end

        dataset.where(args).limit(1).async_all{ |rows|
          if rows.any?
            yield rows.first
          else
            yield nil
          end
        }
        nil
      end
    end
  end
end