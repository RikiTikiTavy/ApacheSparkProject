import com.mySpark.ru.SaveLoad;
import org.junit.Test;
import static org.junit.Assert.*;

public class SaveLoadTest {


    @Test
    public void testCsvParser_Clients(){
        long startTime = System.currentTimeMillis();
        assertEquals("data/clientsParsed.csv", SaveLoad.csvParser("data/clients.csv"));
        long time = System.currentTimeMillis() - startTime;
        System.out.print(time);
    }

    @Test
    public void testCsvParser_Terminals(){
        assertEquals("data/terminalsParsed.csv", SaveLoad.csvParser("data/terminals.csv"));
    }

    @Test
    public void testCsvParser_Transactions(){
        assertEquals("data/transactionsParsed.csv", SaveLoad.csvParser("data/transactions.csv"));
    }

    @Test
    public void testSaveToCsv(){

        assertEquals("data/transactionsParsed.csv", SaveLoad.csvParser("data/transactions.csv"));
    }
}
