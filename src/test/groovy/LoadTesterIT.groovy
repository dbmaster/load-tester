import io.dbmaster.testng.BaseToolTestNGCase;

import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class LoadTesterIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def parameters = [ "p_server"      :  getTestProperty("load-tester.p_server"),
                           "p_total_runs"  : Integer.valueOf(getTestProperty("load-tester.p_total_runs")),
                           "p_concurrent_runs"  :  Integer.valueOf(getTestProperty("load-tester.p_concurrent_runs")),
                           "p_runs_per_thread" :  Integer.valueOf(getTestProperty("load-tester.p_runs_per_thread")),
                           "p_batch_size" :  Integer.valueOf(getTestProperty("load-tester.p_batch_size"))
                         ]
        String result = tools.toolExecutor("load-tester", parameters).execute()
        //assertTrue(result.contains("Source Database"), "Unexpected search results ${result}");
        //assertTrue(result.contains("Target Database"), "Unexpected search results ${result}");
    }
}
