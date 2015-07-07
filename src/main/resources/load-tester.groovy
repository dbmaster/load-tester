import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.service.connection.model.DatabaseConnection

import java.util.Random
import java.sql.CallableStatement
import org.slf4j.Logger
import org.apache.commons.lang.StringUtils
import com.branegy.scripting.DbMaster

LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(p_total_runs.intValue());
ExecutorService service = new ThreadPoolExecutor(p_concurrent_runs.intValue(), 
                                                 p_concurrent_runs.intValue(), 
                                                 20L, 
                                                 TimeUnit.SECONDS, 
                                                 workQueue)
for (int i=0; i<p_total_runs; ++i) {
    service.submit(new ConnectionTask(dbm, 
                                      p_server, 
                                      p_batch_size.intValue(), 
                                      logger)
                   )
}

try {
    service.shutdown()
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
} catch (InterruptedException e) {
    service.shutdownNow()
    while (true) {
        try {
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
            break
        } catch (InterruptedException e2) {
            // no care about multiple stopping
        }
    }
    throw e
}


class ConnectionTask implements Runnable {
    final Random random = new Random()
    final DbMaster dbm
    final String server
    final int batch_size
    final Logger logger
    
    public ConnectionTask(DbMaster dbm, String server, int batch_size, Logger logger) {
       this.dbm = dbm;
       this.server = server
       this.batch_size = batch_size
       this.logger = logger
    }
    
    //private int randomRange(int min, int max){
    //    return random.nextInt(max-min+1)+min;
    //}
    
    @Override
    public void run() {
        try{
            ConnectionService connectionSrv = dbm.getService(ConnectionService.class)
            DatabaseConnection connection = connectionSrv.findByName(server)
            def connector = ConnectionProvider.getConnector(connection)
            
            def jdbcConnection = null
            try {
                jdbcConnection = connector.getJdbcConnection(null)
                jdbcConnection.setAutoCommit(false)
                
                CallableStatement cs;
                cs = jdbcConnection.prepareCall("{call PROCEDURE(?)}")

                for (int j=0; j<p_runs_per_thread; ++j) {
                    if (Thread.currentThread().isInterrupted()) {
                        logger.info("Thread interrupted");
                        return;
                    }
                    cs.setInt(1, random.nextInt(Integer.MAX_VALUE))
                    // TODO Setup parameters                   
                    cs.addBatch();
                    if (j % batch_size == 0) {
                        cs.executeBatch()
                    }    
                }
                if ((p_runs_per_thread-1) % batch_size != 0) {
                    cs.executeBatch()
                }
                cs.close()
                jdbcConnection.commit()
            } finally {
                if (jdbcConnection!=null) {
                    jdbcConnection.close()
                    logger.info("Closing connection")
                }
            }
        } catch (Exception e2) {
            logger.error("Error occured ", e2)
        }
    }
}
