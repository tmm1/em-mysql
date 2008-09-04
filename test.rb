require 'lib/em/mysql'

EM.run{
  SQL = EventedMysql
  def SQL(query, &blk) SQL.select(query, &blk) end
    
  SQL.settings.update :logging => true, :database => 'test', :connections => 15

  SQL.all('set @@wait_timeout = 3')

  n = 0

  SQL.execute('drop table testingabc'){
    SQL.execute('create table testingabc (a int, b int, c int)'){
      EM.add_periodic_timer(0.5) do
        puts;puts
        cur_num = n+=1
        SQL.execute("insert into testingabc values (1,2,#{cur_num})"){
          SQL("select * from testingabc where c = #{cur_num} limit 1"){ |res| puts res.inspect.rjust(100) }
        }
      end
    }
  }
}