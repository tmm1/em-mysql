require 'lib/em/mysql'

EM.run{
  SQL = EventedMysql
  def SQL(query, &blk) SQL.select(query, &blk) end
    
  SQL.settings.update :logging => true,
                      :database => 'test',
                      :connections => 25,
                      :timeout => 3

  if false

    EM.add_timer(2.5){ SQL.all('use test') }

  else

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