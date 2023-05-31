package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.CookieValidation;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: guarda qua e vedi come usarlo bene https://www.tabnine.com/code/java/classes/org.apache.curator.test.TestingServer

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
                e1.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
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
            System.out.println("eccezione: " + e.getMessage());
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
                e.printStackTrace();
            }
        }
    }



}
