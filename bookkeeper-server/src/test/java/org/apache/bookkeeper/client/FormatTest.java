package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;



@RunWith(value= Parameterized.class)
public class FormatTest extends BookKeeperClusterTestCase{

    private boolean expected;
    private boolean conf;
    private boolean isInteractive;
    private boolean force;

    private static final int numOfBookies = 4;
    private final int lostBookieRecoveryDelayInitValue = 1800;
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;

    private Set<Long> ledgerIds = new HashSet<>();


    public static ServerConfiguration getInvalisServerConf(){

        ServerConfiguration invalid_c = new ServerConfiguration();
        invalid_c.setBookiePort(-1); //this is a pot number invalid, so the Server configuration is invalid
        return invalid_c;
    }

    @AfterClass
    public static void cleanup() throws Exception {
        // Ferma il server ZooKeeper locale e rilascia le risorse
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                // for the test with isInteractive = true an use input is expected but is not possible to mock it.
                // Assuming this, all the tests are made using isInteractive = false and the variation is only made ok force

                // true for the ServerConfiguration means that it is valid
                {true, true, false, true},
                // this is expected to be false because force is false and forced is false so the method simply returns false
                {false, true, false, false},

                // false for the ServerConfiguration means that it is invalid
                {false, false, false, true},
                {false, false, false, false},

                // so for all the test is meaningfull to check if metadata have been deleted after the call
        });
    }


    public FormatTest(boolean expected, boolean conf, boolean isInteractive, boolean force) {
        super(numOfBookies);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);

        this.expected = expected;
        this.conf = conf;
        this.isInteractive = isInteractive;
        this.force = force;

    }


    @Before
    public void setUp() throws Exception {
        try {

            super.setUp();
        }catch (Exception e){ e.printStackTrace(); }

        try {
            ClientConfiguration conf = new ClientConfiguration();
            conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

            /*
             * Creazione di un servizio BookKeeper e l'aggiunta di 2 ledgers
             */
            int numOfLedgers = 2;
            try (BookKeeper bkc = new BookKeeper(conf)) {

                for (int n = 0; n < numOfLedgers; n++) {
                    try (LedgerHandle lh = bkc.createLedger(numOfBookies, numOfBookies, digestType, "L".getBytes())) {
                        ledgerIds.add(lh.getId());
                        lh.addEntry("000".getBytes());
                    }
                }
            }
            catch(Exception e1){
                //e1.printStackTrace();
            }

        } catch (Exception e) {
            //e.printStackTrace();
        }

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


    @Test
    public void testFormat() {
        boolean ret;
        int numOfLedgers = 2;

        try {
            if(this.conf) {
                ret = BookKeeperAdmin.format(baseConf, this.isInteractive, this.force);
            }else {
                ret = BookKeeperAdmin.format(null, this.isInteractive, this.force);
            }
        } catch (Exception e) {
            ret = false;
        }

        assertEquals(expected, ret);

        // for checking if format has performed the clean it is enough to create a new ledger
        // and check if it's created with the same id as the previouse one

        for (int n = 0; n < numOfLedgers; n++) {
            try (LedgerHandle lh = bkc.createLedger(numOfBookies, numOfBookies, digestType, "L".getBytes())) {
                lh.addEntry("000".getBytes());
                assertTrue(ledgerIds.contains(lh.getId()));
            } catch (BKException | InterruptedException e) {
                //e.printStackTrace();
            }
        }
    }



}
