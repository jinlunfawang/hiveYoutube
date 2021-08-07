import com.atguigu.videoETL2.ETLMap;
import org.junit.Test;

/**
 * Created by Liang on 2020/3/1.
 */

public class testETL {
@Test
public void test1(){

    ETLMap etlMap = new ETLMap();
    String s = etlMap.etldata("SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUks");
    System.out.println(s);
}

}
