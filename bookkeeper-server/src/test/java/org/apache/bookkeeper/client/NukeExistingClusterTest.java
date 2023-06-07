package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(value= Parameterized.class)
public class NukeExistingClusterTest extends BookKeeperClusterTestCase {

    public static enum confEnum {
        VALID,
        INVALID,
        NULL
    }

    private boolean expected;
    private confEnum conf;
    private String ledgerRootPath;
    private String instanceId;
    private boolean force;

    private static confEnum invalid_c;
    private static ServerConfiguration valid_c;

    private static String valid_iId;
    private static String inv_iId;

    private static final int numOfBookies = 4;
    private final int lostBookieRecoveryDelayInitValue = 1800;
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private Set<Long> ledgerIds = new HashSet<>();




    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                {false, confEnum.INVALID, "/ledgers", valid_iId, true},
                {false, confEnum.INVALID, "/ledgers", inv_iId, false},
                {false, confEnum.INVALID, "/ledgers", null, false},
                {false, confEnum.INVALID, "/ledgers", "", false},

                {false, confEnum.VALID, ".../", valid_iId, true},
                {false, confEnum.VALID, ".../", inv_iId, false},
                {false, confEnum.VALID, ".../", null, false},
                {false, confEnum.VALID, ".../", "", false},

                {false, confEnum.NULL, "/ledgers", valid_iId, true},
                {false, confEnum.NULL, "/ledgers", inv_iId, false},
                {false, confEnum.NULL, "/ledgers", null, false},
                {false, confEnum.NULL, "/ledgers", "", false},

                {false, confEnum.VALID, "", valid_iId, true},
                {false, confEnum.VALID, "", inv_iId, false},
                {false, confEnum.VALID, "", null, false},
                {false, confEnum.VALID, "", "", false},

        });
    }


    public NukeExistingClusterTest(boolean expected, confEnum conf, String ledgerRootPath, String instanceId, boolean force) {
        super(numOfBookies);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);

        this.expected = expected;
        this.conf = conf;
        this.ledgerRootPath = ledgerRootPath;
        this.instanceId = instanceId;
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
                e1.printStackTrace();
            }

        } catch (Exception e) {
            //e.printStackTrace();
        }


    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

/*
    @Ignore
*/
    @Test
    public void testNukeExistingCluster() {
        boolean ret;

        // I need to create a new ServerConfiguration (based on baseConf) because baseConf is final
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);


        try {
            if (this.conf.equals(confEnum.VALID)) {

                newConfig.setMetadataServiceUri(newMetadataServiceUri(this.ledgerRootPath));
                //create a valid istanceId
                byte[] data = zkc.getData(
                        ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf) + "/" + BookKeeperConstants.INSTANCEID,
                        false, null);
                valid_iId = new String(data, UTF_8);

                baseConf.setMetadataServiceUri(newMetadataServiceUri(this.ledgerRootPath));

                ret = BookKeeperAdmin.nukeExistingCluster(baseConf, this.ledgerRootPath, this.instanceId, this.force);

            } else if (this.conf.equals(confEnum.INVALID)) {

                newConfig.setMetadataServiceUri(newMetadataServiceUri("\\wrong_path"));
                newConfig.setBookiePort(-100);
                newConfig.setMetadataServiceUri("wrong...");


                ret = BookKeeperAdmin.nukeExistingCluster(newConfig, this.ledgerRootPath, this.instanceId, this.force);

            } else {

                newConfig = null;
                ret = BookKeeperAdmin.nukeExistingCluster(newConfig, this.ledgerRootPath, this.instanceId, this.force);

            }

        } catch (Exception e) {
            ret = false;
        }

        assertEquals(expected, ret);
    }
}
