package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.zookeeper.KeeperException;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(value= Parameterized.class)
public class NukeExistingClusterTest extends BookKeeperClusterTestCase {

    public static enum confEnum {
        VALID,
        INVALID,
        NULL
    }

    public static enum instanceIdEnum {
        VALID,
        INVALID,
        NULL
    }

    private boolean expected;
    private confEnum conf;
    private String ledgerRootPath;
    private instanceIdEnum instanceId;
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
                /*
                * {invalid_c, "/ledgers"}, {valid_c, "/roothpath"}, {null, "/ledgers"}, {valid_c, null}
                *
                * {invalid_Id, true}, {valid_id, false}, {null, false}
                * */

                {false, confEnum.INVALID, "/ledgers", instanceIdEnum.VALID, false},
                {false, confEnum.INVALID, "/ledgers", instanceIdEnum.INVALID, true},
                {false, confEnum.INVALID, "/ledgers", instanceIdEnum.NULL, false},

                {false, confEnum.VALID, "/rootpath", instanceIdEnum.VALID, false},
                {false, confEnum.VALID, "/rootpath", instanceIdEnum.INVALID, true},
                {false, confEnum.VALID, "/rootpath", instanceIdEnum.NULL, false},

                {false, confEnum.VALID, null, instanceIdEnum.VALID, false},
                {false, confEnum.VALID, null, instanceIdEnum.INVALID, true},
                {false, confEnum.VALID, null, instanceIdEnum.NULL, false},

                {false, confEnum.NULL, "/ledgers", instanceIdEnum.VALID, false},
                {false, confEnum.NULL, "/ledgers", instanceIdEnum.INVALID, true},
                {false, confEnum.NULL, "/ledgers", instanceIdEnum.NULL, false},

                {true, confEnum.VALID, "/ledgers", instanceIdEnum.VALID, false},
                {true, confEnum.VALID, "/ledgers", instanceIdEnum.INVALID, true},
                {false, confEnum.VALID, "/ledgers", instanceIdEnum.NULL, false},

        });
    }


    public NukeExistingClusterTest(boolean expected, confEnum conf, String ledgerRootPath, instanceIdEnum instanceId, boolean force) {
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
                        System.out.println("ledgers Id: " + lh.getId());
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

    @Test
    public void testNukeExistingCluster() throws InterruptedException, KeeperException {
        boolean ret;

        // I need to create a new ServerConfiguration (based on baseConf) because baseConf is final
        ServerConfiguration validConf = new ServerConfiguration(baseConf);

        validConf.setMetadataServiceUri(newMetadataServiceUri("/ledgers"));
        //create a valid istanceId
        byte[] data = zkc.getData(
                ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf) + "/" + BookKeeperConstants.INSTANCEID,
                false, null);
        valid_iId = new String(data, UTF_8);

        inv_iId = "7940c93b-5da8-4fa7-941e-d254d678fb1c"; // a random id that is different from the real one



        try {
            if (this.conf.equals(confEnum.VALID)) {

                if(this.instanceId.equals(instanceIdEnum.VALID))
                    ret = BookKeeperAdmin.nukeExistingCluster(validConf, this.ledgerRootPath, valid_iId, this.force);
                else if(this.instanceId.equals(instanceIdEnum.INVALID))
                    ret = BookKeeperAdmin.nukeExistingCluster(validConf, this.ledgerRootPath, inv_iId, this.force);
                else
                    ret = BookKeeperAdmin.nukeExistingCluster(validConf, this.ledgerRootPath, null, this.force);

            } else if (this.conf.equals(confEnum.INVALID)) {

                validConf.setBookiePort(-100); // make the configuration invalid
                validConf.setMetadataServiceUri(newMetadataServiceUri(null));

                if(this.instanceId.equals(instanceIdEnum.VALID))
                    ret = BookKeeperAdmin.nukeExistingCluster(validConf, this.ledgerRootPath, valid_iId, this.force);
                else if(this.instanceId.equals(instanceIdEnum.INVALID))
                    ret = BookKeeperAdmin.nukeExistingCluster(validConf, this.ledgerRootPath, inv_iId, this.force);
                else
                    ret = BookKeeperAdmin.nukeExistingCluster(validConf, this.ledgerRootPath, null, this.force);

            } else {

                if(this.instanceId.equals(instanceIdEnum.VALID))
                    ret = BookKeeperAdmin.nukeExistingCluster(null, this.ledgerRootPath, valid_iId, this.force);
                else if(this.instanceId.equals(instanceIdEnum.INVALID))
                    ret = BookKeeperAdmin.nukeExistingCluster(null, this.ledgerRootPath, inv_iId, this.force);
                else
                    ret = BookKeeperAdmin.nukeExistingCluster(null, this.ledgerRootPath, null, this.force);

            }

        } catch (Exception e) {
            ret = false;
        }

        System.out.println("Test case: " + this.conf.toString() + " " + this.ledgerRootPath + " " + this.instanceId.toString() + " " + this.force);
        assertEquals(expected, ret);
    }
}
