package org.apache.bookkeeper.client;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(value= Suite.class)
@Suite.SuiteClasses(value={NukeExistingClusterTest.class, FormatTest.class})
public class BookKeeperAdminTestSuite {

}
