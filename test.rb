require 'lib/em/mysql'

# EM.kqueue
# EM.epoll
EM.run{
  EM.start_server '127.0.0.1', 12345 do |c|
    def c.receive_data data
      p 'sending http response'
      send_data "hello"
      close_connection_after_writing
    end
  end

  SQL = EventedMysql
  def SQL(query, &blk) SQL.select(query, &blk) end

  if true

    SQL.settings.update :logging => true,
                        :database => 'test',
                        :connections => 1

    SQL.execute('select 1+2')

    EM.add_timer(1){
      3.times do SQL.select('select sleep(0.5)+1'){|r| p(r) } end
    }

  elsif false

    SQL.settings.update :logging => true,
                        :database => 'test',
                        :connections => 10

    EM.add_timer(2.5){ SQL.all('use test') }

  else

    SQL.settings.update :logging => true,
                        :database => 'test',
                        :connections => 10,
                        :timeout => 1

    n = 0

    SQL.execute('drop table if exists testingabc'){
      SQL.execute('create table testingabc (a int, b int, c int)'){
        EM.add_periodic_timer(0.2) do
          cur_num = n+=1
          SQL.execute("insert into testingabc values (1,2,#{cur_num})"){
            SQL("select * from testingabc where c = #{cur_num} limit 1"){ |res| puts;puts }
          }
        end
      }
    }

  end

}