package com.springyweb.alfresco.tests.cmis;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.client.api.Repository;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CMISTest {
  private static final String CMIS_ENDPOINT_TEST_SERVER = "http://localhost:8080/alfresco/s/api/cmis";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin";

  private static final String TEST_FOLDER_NAME = "test_folder";
  private static final String TEST_CMIS_DOCUMENT_TYPE = "swct:document";
  private static final String TEST_CMIS_PROPERY_SINGLE_INT = "swct:propSingleInt";
  private static final String TEST_CMIS_PROPERY_SINGLE_DOUBLE = "swct:propSingleDouble";
  private static final String TEST_CMIS_PROPERY_SINGLE_BOOLEAN = "swct:propSingleBoolean";

  private Folder root = null;
  private Folder testRootFolder = null;

  private Session session;

  @Before
  public void setup() {
    final Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(SessionParameter.USER, USERNAME);
    parameters.put(SessionParameter.PASSWORD, PASSWORD);
    parameters.put(SessionParameter.ATOMPUB_URL, CMIS_ENDPOINT_TEST_SERVER);
    parameters.put(SessionParameter.BINDING_TYPE,
        BindingType.ATOMPUB.value());

    // Create a session with the client-side cache disabled.
    final SessionFactoryImpl sessionFactory = SessionFactoryImpl.newInstance();
    final Repository repository = sessionFactory.getRepositories(parameters).get(0);
    session = repository.createSession();
    session.getDefaultContext().setCacheEnabled(false);
    root = session.getRootFolder();
    // Create the test folder. Delete it if it already exists.
    testRootFolder = getFolderByPath(root.getPath() + TEST_FOLDER_NAME);
    if (testRootFolder != null) {
      testRootFolder.deleteTree(true, UnfileObject.DELETE, true);
    }

    testRootFolder = createNamedFolder(root, TEST_FOLDER_NAME);

  }

  @After
  public void tearDown() {
    if (testRootFolder != null) {
      System.out.println("Removing test data");
      try {
        testRootFolder.deleteTree(true, UnfileObject.DELETE, true);
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void folderSearches() {

    final String allFoldersByNameQuery = "SELECT * FROM cmis:folder WHERE cmis:name='%s'";
    final String allFoldersInFolderQuery = "SELECT * FROM cmis:folder WHERE in_folder('%s') and cmis:name='%s'";
    final String allFoldersInTreeQuery = "SELECT * FROM cmis:folder WHERE in_tree('%s') and cmis:name='%s'";

    final String testFolderName = "my_test_folder";
    final Folder testFolder = createNamedFolder(testRootFolder, testFolderName);

    // Check that only one folder with the search name can be found
    assertEquals("Wrong result count", 1,
      queryResultCount(String.format(allFoldersByNameQuery, testFolderName), false));

    // Create a sub-folder folder of the same name
    createNamedFolder(testFolder, testFolderName);

    // Check that 2 folders with the search name can be found
    assertEquals("Wrong result count", 2,
      queryResultCount(String.format(allFoldersByNameQuery, testFolderName), false));

    // Now search again this time limit the search to folders IMMEADIATELY
    // within the test root space using in_folder

    assertEquals(
      "Wrong result count",
      1,
      queryResultCount(
        String.format(allFoldersInFolderQuery, testRootFolder.getId(), testFolderName),
        false));

    // Now search again this time limit the search to ANYWHERE beneath the test
    // root space using in_tree

    assertEquals(
      "Wrong result count",
      2,
      queryResultCount(
        String.format(allFoldersInTreeQuery, testRootFolder.getId(), testFolderName),
        false));
  }

  @Test
  public void comparisonPredicatesString() {

    // Note:Strings collation is case sensitive and space/pad sensitive
    // Collation is the default Java String collation Setup documents
    // There A and B < a

    createNamedDocument(testRootFolder, "a");
    createNamedDocument(testRootFolder, "Ab");
    createNamedDocument(testRootFolder, "b");
    createNamedDocument(testRootFolder, "Ba");

    final String nameEqualsQuery = "SELECT * from cmis:document where in_folder('%s') and cmis:name =  '%s'";
    assertEquals("Wrong result count", 1,
      queryResultCount(String.format(nameEqualsQuery, testRootFolder.getId(), "a"), false));

    final String nameNotEqualsQuery = "SELECT * from cmis:document where in_folder('%s') and cmis:name <>  '%s'";
    assertEquals("Wrong result count", 3,
      queryResultCount(String.format(nameNotEqualsQuery, testRootFolder.getId(), "a"), false));


    final String nameLessThanQuery = "SELECT * from cmis:document where in_folder('%s') and cmis:name <  '%s'";
    assertEquals("Wrong result count", 2,
      queryResultCount(String.format(nameLessThanQuery, testRootFolder.getId(), "a"), false));

    final String nameLessThanOrEqualToQuery = "SELECT * from cmis:document where in_folder('%s') and cmis:name <=  '%s'";
    assertEquals(
      "Wrong result count",
      3,
      queryResultCount(String.format(nameLessThanOrEqualToQuery, testRootFolder.getId(), "a"),
        false));

    final String nameGreaterThanQuery = "SELECT * from cmis:document where in_folder('%s') and cmis:name >  '%s'";
    assertEquals("Wrong result count", 1,
      queryResultCount(String.format(nameGreaterThanQuery, testRootFolder.getId(), "a"), false));

    final String nameGreaterThanOrEqualToQuery = "SELECT * from cmis:document where in_folder('%s') and cmis:name >=  '%s'";
    assertEquals(
      "Wrong result count",
      2,
      queryResultCount(String.format(nameGreaterThanOrEqualToQuery, testRootFolder.getId(), "a"),
        false));

  }

  @Test
  public void comparisonPredicatesInteger() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 4 test docs
    for (int i = 1; i <= 4; i++) {
      props.clear();
      props.put(TEST_CMIS_PROPERY_SINGLE_INT, i);
      createTestCMISDocument(testRootFolder, "test" + i, props);
    }

    final String equalsQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_INT + " =  %d";
    assertEquals("Wrong result count", 1,
      queryResultCount(String.format(equalsQuery, testRootFolder.getId(), 1), false));

    final String notEqualsQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_INT + " <>  %d";
    assertEquals("Wrong result count", 3,
      queryResultCount(String.format(notEqualsQuery, testRootFolder.getId(), 1), false));


    final String lessThanQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_INT + " <  %d";
    assertEquals("Wrong result count", 2,
      queryResultCount(String.format(lessThanQuery, testRootFolder.getId(), 3), false));

    final String lessThanOrEqualToQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_INT + "<=  %d";
    assertEquals(
      "Wrong result count",
      3,
      queryResultCount(String.format(lessThanOrEqualToQuery, testRootFolder.getId(), 3),
        false));

    final String greaterThanQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_INT + " >  %d";
    assertEquals("Wrong result count", 3,
      queryResultCount(String.format(greaterThanQuery, testRootFolder.getId(), 1), false));

    final String greaterThanOrEqualToQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_INT + " >=  %d";

    assertEquals(
      "Wrong result count",
      4,
      queryResultCount(String.format(greaterThanOrEqualToQuery, testRootFolder.getId(), 1),
        false));
  }

  @Test
  public void comparisonPredicatesDecimal() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 4 test docs
    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.0);
    createTestCMISDocument(testRootFolder, "test1", props);

    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.1);
    createTestCMISDocument(testRootFolder, "test2", props);

    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.2);
    createTestCMISDocument(testRootFolder, "test3", props);

    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.3);
    createTestCMISDocument(testRootFolder, "test4", props);

    final String equalsQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_DOUBLE + " =  1.0";
    assertEquals("Wrong result count", 1,
      queryResultCount(String.format(equalsQuery, testRootFolder.getId()), false));

    final String notEqualsQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_DOUBLE + " <>  1.0";
    assertEquals("Wrong result count", 3,
      queryResultCount(String.format(notEqualsQuery, testRootFolder.getId()), false));


    final String lessThanQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_DOUBLE + " <  1.2";
    assertEquals("Wrong result count", 2,
      queryResultCount(String.format(lessThanQuery, testRootFolder.getId()), false));

    final String lessThanOrEqualToQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_DOUBLE + "<=  1.2";
    assertEquals(
      "Wrong result count",
      3,
      queryResultCount(String.format(lessThanOrEqualToQuery, testRootFolder.getId()),
        false));

    final String greaterThanQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_DOUBLE + " >  1.0";
    assertEquals("Wrong result count", 3,
      queryResultCount(String.format(greaterThanQuery, testRootFolder.getId()), false));

    final String greaterThanOrEqualToQuery = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
      + " where in_folder('%s') and "
      + TEST_CMIS_PROPERY_SINGLE_DOUBLE + " >=  1.0";

    assertEquals(
      "Wrong result count",
      4,
      queryResultCount(String.format(greaterThanOrEqualToQuery, testRootFolder.getId()),
        false));
  }

  @Test
  public void comparisonPredicatesBoolean() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 2 test docs with true and false
    props.put(TEST_CMIS_PROPERY_SINGLE_BOOLEAN, true);
    createTestCMISDocument(testRootFolder, "test1", props);

    props.put(TEST_CMIS_PROPERY_SINGLE_BOOLEAN, false);
    createTestCMISDocument(testRootFolder, "test2", props);

    final String[] testVals = { "true", "TRUE", "false", "FALSE" };
    for (final String val: testVals) {
      final String query = "SELECT * from " + TEST_CMIS_DOCUMENT_TYPE
        + " where in_folder('%s') and "
        + TEST_CMIS_PROPERY_SINGLE_BOOLEAN + " =  %s";

      assertEquals("Wrong result count", 1,
        queryResultCount(String.format(query, testRootFolder.getId(), val), false));

    }
  }

  private long queryResultCount(final String query, final boolean searchAllVersions) {
    return executeQuery(query, searchAllVersions).getTotalNumItems();
  }

  private ItemIterable<QueryResult> executeQuery(final String query, final boolean searchAllVersions) {
    System.out.println("Executing query " + query + " (Searching all versions: "
      + searchAllVersions + ")");
    return session.query(query, false);
  }

  /**
   * 
   * @param path
   * @return The folder at $path or null
   * @throws CmisRuntimeException
   *           If the cmisObject at $path is not a folder
   */
  private Folder getFolderByPath(final String path) throws CmisRuntimeException {
    Folder folder = null;
    try {
      final CmisObject cmisObject = session.getObjectByPath(path);
      if (cmisObject.getBaseTypeId() != BaseTypeId.CMIS_FOLDER) {
        throw new CmisRuntimeException("Object with path '" + path
          + "' is not of type 'cmis:folder'.");
      }
      folder = (Folder)cmisObject;
    } catch (final CmisObjectNotFoundException ignored) {
    }
    return folder;
  }

  /**
   * Create a new child folder of type cmis:folder
   * 
   * @param parent
   *          The parent folder
   * @param name
   *          The name of the new folder
   * @return
   */
  private Folder createNamedFolder(final Folder parent, final String name) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(PropertyIds.NAME, name);
    props.put(PropertyIds.OBJECT_TYPE_ID, BaseTypeId.CMIS_FOLDER.value());
    return parent.createFolder(props);
  }

  /**
   * Create a new child document of type cmis:document
   * 
   * @param parent
   *          The parent folder
   * @param name
   *          The name of the new folder
   * @return
   */
  private Document createNamedDocument(final Folder parent, final String name) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(PropertyIds.NAME, name);
    props.put(PropertyIds.OBJECT_TYPE_ID, BaseTypeId.CMIS_DOCUMENT.value());
    return parent.createDocument(props, null, null);
  }

  private Document createTestCMISDocument(final Folder parent, final String name,
    final Map<String, Object> props) {

    props.put(PropertyIds.NAME, name);
    props.put(PropertyIds.OBJECT_TYPE_ID, "D:" + TEST_CMIS_DOCUMENT_TYPE);
    System.out.println("Creating new doc " + props);
    return parent.createDocument(props, null, null);
  }
}