package com.springyweb.alfresco.tests.cmis;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.alfresco.cmis.client.AlfrescoDocument;
import org.alfresco.util.ISO8601DateFormat;
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
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CMISTest {
  private static final String CMIS_ENDPOINT_TEST_SERVER = "http://localhost:8080/alfresco/s/api/cmis";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin";

  private static final String TEST_FOLDER_NAME = "test_folder";
  private static final String TEST_CMIS_DOCUMENT_TYPE = "swct:document";
  private static final String TEST_CMIS_FOLDER_TYPE = "swct:folder";

  private static final String ASPECT_TITLED = "cm:titled";
  private static final String PROPERTY_DESCRIPTION = "cm:description";

  private static final String TEST_CMIS_PROPERY_SINGLE_INT = "swct:propSingleInt";
  private static final String TEST_CMIS_PROPERY_SINGLE_DOUBLE = "swct:propSingleDouble";
  private static final String TEST_CMIS_PROPERY_SINGLE_BOOLEAN = "swct:propSingleBoolean";
  private static final String TEST_CMIS_PROPERY_SINGLE_DATE_TIME = "swct:propSingleDateTime";
  private static final String TEST_CMIS_PROPERY_SINGLE_STRING = "swct:propSingleString";

  private static final String TEST_CMIS_PROPERY_MULTIPLE_STRING = "swct:propMultipleString";
  private static final String TEST_CMIS_PROPERY_MULTIPLE_INT = "swct:propMultipleInt";
  private static final String TEST_CMIS_PROPERY_MULTIPLE_DOUBLE = "swct:propMultipleDouble";
  private static final String TEST_CMIS_PROPERY_MULTIPLE_BOOLEAN = "swct:propMultipleBoolean";
  private static final String TEST_CMIS_PROPERY_MULTIPLE_DATE_TIME = "swct:propMultipleDateTime";

  // Note the replaceable parameters here are (in order) folder id,property,predicate,value
  // e.g SELECT * from swct:document where in_folder('workspace://SpacesStore/c22f856c-6cec-4e16-9c1c-60df621bba16') and swct:propSingleString = 'b'
  private static final String PREDICATE_QUERY_TEMPLATE_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s '%s'";

  private static final String PREDICATE_QUERY_TEMPLATE_BOOLEAN = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s %b";

  private static final String PREDICATE_QUERY_TEMPLATE_INTEGER = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s %d";

  // Note: We only use 1 decimal place e.g 1.0 not 1 or 1.0000
  private static final String PREDICATE_QUERY_TEMPLATE_DECIMAL = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s %.2g%n";

  private static final String PREDICATE_QUERY_TEMPLATE_DATETIME = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s TIMESTAMP '%s'";

  private static final String PREDICATE_QUERY_TEMPLATE_UNQUOTED_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s %s";

  // Note the replaceable parameters here are (in order) folder id,sting1,string2
  // SELECT * from swct:document where in_folder('workspace://SpacesStore/c22f856c-6cec-4e16-9c1c-60df621bba16') and swct:propSingleBoolean IS NULL
  private static final String TWO_VAL_PREDICATE_QUERY_TEMPLATE_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s";

  // Note the replaceable parameters here are (in order) folder id,string1
  // SELECT * from swct:document where in_folder('workspace://SpacesStore/c22f856c-6cec-4e16-9c1c-60df621bba16') and CONTAINS('foo')
  private static final String SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s";

  // e.g SELECT * from swct:document where in_folder('workspace://SpacesStore/c22f856c-6cec-4e16-9c1c-60df621bba16') and 'foo' = ANY
  // swct:propSingleBoolean
  private static final String PREDICATE_QUANTIFIED_QUERY_TEMPLATE_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and '%s' %s %s";

  private static final String PREDICATE_QUANTIFIED_QUERY_TEMPLATE_INTEGER = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %d %s %s";

  private static final String PREDICATE_QUANTIFIED_QUERY_TEMPLATE_BOOLEAN = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %b %s %s";

  private static final String PREDICATE_QUANTIFIED_QUERY_TEMPLATE_DECIMAL = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %.2g%n %s %s";

  private static final String PREDICATE_QUANTIFIED_QUERY_TEMPLATE_DATETIME = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and TIMESTAMP '%s' %s %s";

  // e.g SELECT * from swct:document WHERE ANY swct:propSingleBoolean IN (true, false)
  private static final String PREDICATE_QUANTIFIED_IN_TEMPLATE_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and " + Predicate.ANY + " %s "
    + Predicate.IN + "%s";

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

    // Set the alfresco object factory
    parameters.put(SessionParameter.OBJECT_FACTORY_CLASS,
      "org.alfresco.cmis.client.impl.AlfrescoObjectFactoryImpl");


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

    testRootFolder = createTestCMISFolder(root, TEST_FOLDER_NAME);

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
  public void comparisonPredicatesString() {

    // Create test documents with a single string property
    final Map<String, Object> props = new HashMap<String, Object>();

    props.put(TEST_CMIS_PROPERY_SINGLE_STRING, "b");
    final String id_b = createTestCMISDocument(testRootFolder, "testb", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_STRING, "Bc");
    final String id_Bc = createTestCMISDocument(testRootFolder, "testBc", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_STRING, "c");
    final String id_c = createTestCMISDocument(testRootFolder, "testc", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_STRING, "Cb");
    final String id_Cb = createTestCMISDocument(testRootFolder, "testCb", props).getId();

    final String expecedId = id_b;
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_STRING, expecedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.EQUALS.getSymbol(), "b");

    Set<String> expectedIds = toSet(id_Bc, id_c, id_Cb);

    assertQueryResults(PREDICATE_QUERY_TEMPLATE_STRING, expectedIds, testRootFolder.getId(),
        TEST_CMIS_PROPERY_SINGLE_STRING,
        Predicate.NOT_EQUALS.getSymbol(), "b");

    expectedIds = toSet(id_b, id_Bc);

    assertQueryResults(PREDICATE_QUERY_TEMPLATE_STRING, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.LESS_THAN.getSymbol(), "c");

    expectedIds = toSet(id_b, id_Bc, id_c);

    assertQueryResults(PREDICATE_QUERY_TEMPLATE_STRING, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), "c");

    expectedIds = toSet(id_c, id_Bc, id_Cb);

    assertQueryResults(PREDICATE_QUERY_TEMPLATE_STRING, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.GREATER_THAN.getSymbol(), "b");

    expectedIds = toSet(id_b, id_c, id_Bc, id_Cb);

    assertQueryResults(PREDICATE_QUERY_TEMPLATE_STRING, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), "b");
  }

  @Test
  public void comparisonPredicatesInteger() {

    final Map<String, Object> props = new HashMap<String, Object>();

    props.put(TEST_CMIS_PROPERY_SINGLE_INT, 1);
    final String id_1 = createTestCMISDocument(testRootFolder, "test1", props).getId();
    props.put(TEST_CMIS_PROPERY_SINGLE_INT, 2);
    final String id_2 = createTestCMISDocument(testRootFolder, "test2", props).getId();
    props.put(TEST_CMIS_PROPERY_SINGLE_INT, 3);
    final String id_3 = createTestCMISDocument(testRootFolder, "test3", props).getId();
    props.put(TEST_CMIS_PROPERY_SINGLE_INT, 4);
    final String id_4 = createTestCMISDocument(testRootFolder, "test4", props).getId();

    final String expectedId = id_1;
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_INTEGER, expectedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.EQUALS.getSymbol(), 1);

    Set<String> expectedIds = toSet(id_2, id_3, id_4);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_INTEGER, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.NOT_EQUALS.getSymbol(), 1);

    expectedIds = toSet(id_1, id_2);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_INTEGER, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.LESS_THAN.getSymbol(), 3);

    expectedIds = toSet(id_1, id_2, id_3);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_INTEGER, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), 3);

    expectedIds = toSet(id_2, id_3, id_4);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_INTEGER, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.GREATER_THAN.getSymbol(), 1);

    expectedIds = toSet(id_1, id_2, id_3, id_4);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_INTEGER, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), 1);
  }

  @Test
  public void comparisonPredicatesDecimal() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 4 test docs
    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.0);
    final String id_1_0 = createTestCMISDocument(testRootFolder, "test1", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.1);
    final String id_1_1 = createTestCMISDocument(testRootFolder, "test2", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.2);
    final String id_1_2 = createTestCMISDocument(testRootFolder, "test3", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_DOUBLE, 1.3);
    final String id_1_3 = createTestCMISDocument(testRootFolder, "test4", props).getId();

    final String expectedId = id_1_0;

    assertQueryResult(PREDICATE_QUERY_TEMPLATE_DECIMAL, expectedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.EQUALS.getSymbol(), 1.0);

    Set<String> expectedIds = toSet(id_1_1, id_1_2, id_1_3);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DECIMAL, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.NOT_EQUALS.getSymbol(), 1.0);

    expectedIds = toSet(id_1_0, id_1_1);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DECIMAL, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.LESS_THAN.getSymbol(), 1.2);

    expectedIds = toSet(id_1_0, id_1_1, id_1_2);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DECIMAL, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), 1.2);

    expectedIds = toSet(id_1_1, id_1_2, id_1_3);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DECIMAL, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.GREATER_THAN.getSymbol(), 1.0);

    expectedIds = toSet(id_1_0, id_1_1, id_1_2, id_1_3);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DECIMAL, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), 1.0);
  }

  @Test
  public void comparisonPredicatesBoolean() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 2 test docs with true and false
    props.put(TEST_CMIS_PROPERY_SINGLE_BOOLEAN, true);
    final String id_true = createTestCMISDocument(testRootFolder, "test1", props).getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_BOOLEAN, false);
    final String id_false = createTestCMISDocument(testRootFolder, "test2", props).getId();

    String expectedId = id_true;

    assertQueryResult(PREDICATE_QUERY_TEMPLATE_BOOLEAN, expectedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_BOOLEAN,
      Predicate.EQUALS.getSymbol(), true);

    expectedId = id_false;

    assertQueryResult(PREDICATE_QUERY_TEMPLATE_BOOLEAN, expectedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_BOOLEAN,
      Predicate.EQUALS.getSymbol(), false);
  }

  /**
   * Note: For the datetime tests to work follow the instructions for changing the lucene analyzer (http://wiki.alfresco.com/wiki/CMIS_Query_Language#
   * Configuring_DateTime_resolution)
   * 
   */
  @Test
  public void comparisonPredicatesDateTime() {
    // Create 4 dates 1 Second apart Note: The time portion of the date is
    // ignored
    final Map<String, Object> props = new HashMap<String, Object>();
    final Date[] dates = new Date[4];
    final String[] ids = new String[4];
    final GregorianCalendar calendar = new GregorianCalendar();

    for (int i = 0; i < dates.length; i++) {
      props.clear();
      dates[i] = calendar.getTime();
      // Note: For OpenCmis to work we add the calendar object not the date (see
      // http://blog.remysaissy.com/2012/04/solving-property-foo-is-datetime.html)
      props.put(TEST_CMIS_PROPERY_SINGLE_DATE_TIME, calendar);
      ids[i] = createTestCMISDocument(testRootFolder, "test" + i, props).getId();
      calendar.add(Calendar.MILLISECOND, 1);
    }

    String expectedId = ids[0];
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_DATETIME, expectedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.EQUALS.getSymbol(), ISO8601DateFormat.format(dates[0]));

    Set<String> expectedIds = toSet(ids[1], ids[2], ids[3]);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DATETIME, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.NOT_EQUALS.getSymbol(), ISO8601DateFormat.format(dates[0]));

    expectedId = ids[0];
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_DATETIME, expectedId, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.LESS_THAN.getSymbol(), ISO8601DateFormat.format(dates[1]));

    expectedIds = toSet(ids[0], ids[1]);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DATETIME, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), ISO8601DateFormat.format(dates[1]));

    expectedIds = toSet(ids[1], ids[2], ids[3]);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DATETIME, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.GREATER_THAN.getSymbol(), ISO8601DateFormat.format(dates[0]));

    expectedIds = toSet(ids[0], ids[1], ids[2], ids[3]);
    assertQueryResults(PREDICATE_QUERY_TEMPLATE_DATETIME, expectedIds, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), ISO8601DateFormat.format(dates[0]));
  }

  @Test
  public void comparisonPredicatesId() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 3 test docs - no properties required for ID testing
    final String id1 = createTestCMISDocument(testRootFolder, "test1", props).getId();
    final String id2 = createTestCMISDocument(testRootFolder, "test2", props).getId();

    String expectedId = id1;
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId, testRootFolder.getId(),
      PropertyIds.OBJECT_ID,
      Predicate.EQUALS.getSymbol(), id1);

    expectedId = id2;
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId, testRootFolder.getId(),
      PropertyIds.OBJECT_ID,
      Predicate.NOT_EQUALS.getSymbol(), id1);
  }

  @Test
  public void inPredicatesString() {
    final TreeSet<Object> allTokens = new TreeSet<Object>();
    allTokens.add("foo");
    allTokens.add("bar");
    allTokens.add("baz");

    testInPredicate(allTokens, TEST_CMIS_PROPERY_SINGLE_STRING, true);
  }

  @Test
  public void inPredicatesInteger() {
    final TreeSet<Object> allTokens = new TreeSet<Object>();
    allTokens.add(1);
    allTokens.add(2);
    allTokens.add(3);

    testInPredicate(allTokens, TEST_CMIS_PROPERY_SINGLE_INT, false);
  }

  @Test
  public void inPredicatesDecimal() {
    final TreeSet<Object> allTokens = new TreeSet<Object>();
    allTokens.add(1.0);
    allTokens.add(2.1);
    allTokens.add(3.2);

    testInPredicate(allTokens, TEST_CMIS_PROPERY_SINGLE_DOUBLE, false);
  }

  @Test
  public void inPredicatesDateTime() {
    final Map<String, Object> props = new HashMap<String, Object>();
    final TreeSet<GregorianCalendar> allCals = new TreeSet<GregorianCalendar>();
    final TreeSet<GregorianCalendar> searchCals = new TreeSet<GregorianCalendar>();
    final TreeSet<Object> searchTokens = new TreeSet<Object>();

    final Set<String> expectedIds = new HashSet<String>();

    final GregorianCalendar calendar = new GregorianCalendar();

    for (int i = 0; i < 4; i++) {
      props.clear();
      allCals.add((GregorianCalendar)calendar.clone());
      calendar.add(Calendar.MILLISECOND, 1);
    }

    searchCals.addAll(allCals);
    searchCals.remove(searchCals.last());

    int counter = 0;
    for (final GregorianCalendar cal: allCals) {
      props.put(TEST_CMIS_PROPERY_SINGLE_DATE_TIME, cal);
      final Document document = createTestCMISDocument(testRootFolder, "test" + counter++, props);
      if (searchCals.contains(cal)) {
        expectedIds.add(document.getId());
        searchTokens.add(ISO8601DateFormat.format(cal.getTime()));
      }
    }

    testInPredicateValues(searchTokens, expectedIds, TEST_CMIS_PROPERY_SINGLE_DATE_TIME, true);
  }

  @Test
  public void likePredicate() {

    // note: The like predicate only applies to strings
    final Map<String, Object> props = new HashMap<String, Object>();
    String expectedId = createTestCMISDocument(testRootFolder, "test", props).getId();

    // All of these string should find test
    final String[] testVals = { "%test%", "test", "t%t", "t__t" };

    for (final String testVal: testVals) {
      assertQueryResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId,
        testRootFolder.getId(),
        PropertyIds.NAME, Predicate.LIKE.getSymbol(), testVal);
    }

    // Test escaping %
    expectedId = createTestCMISDocument(testRootFolder, "t%t", props).getId();
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId,
      testRootFolder.getId(),
      PropertyIds.NAME, Predicate.LIKE.getSymbol(), "t\\%t");

    // Test escaping _
    expectedId = createTestCMISDocument(testRootFolder, "t__t", props).getId();
    assertQueryResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId,
      testRootFolder.getId(),
      PropertyIds.NAME, Predicate.LIKE.getSymbol(), "t\\_\\_t");
  }

  /**
   * CMIS does not support null values for properties. Properties may be set or not set. The null predicate tests if the property is set or unset.
   * This applies to both single valued and multi-valued properties. For multi-valued properties if the property is set it is NOT NULL: the values of
   * the multi-valued properties are not important. This differs from what you would expect from the SQL-92 specification.
   */
  @Test
  public void nullPredicate() {

    final Map<String, Object> props = new HashMap<String, Object>();
    final String idWithoutPropertySet = createTestCMISDocument(testRootFolder, "test", props)
      .getId();

    props.put(TEST_CMIS_PROPERY_SINGLE_BOOLEAN, true);
    final String idWithPropertySet = createTestCMISDocument(testRootFolder, "test2", props)
      .getId();

    // Test for the item without the property set using IS NULL
    assertQueryResult(TWO_VAL_PREDICATE_QUERY_TEMPLATE_STRING, idWithoutPropertySet,
      testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_BOOLEAN, Predicate.IS_NULL.getSymbol());

    // Test for the item with the property set using IS NOT NULL
    assertQueryResult(TWO_VAL_PREDICATE_QUERY_TEMPLATE_STRING, idWithPropertySet,
      testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_BOOLEAN, Predicate.IS_NOT_NULL.getSymbol());
  }

  @Test
  public void testQuantifiedComparisonPredicateString() {

    final List<Object> values = Arrays.asList(new Object[] { "foo", "bar", "baz" });
    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_STRING, values);

    testQuantifiedComparisonPredicate(props, values, PREDICATE_QUANTIFIED_QUERY_TEMPLATE_STRING,
      TEST_CMIS_PROPERY_MULTIPLE_STRING);
  }

  @Test
  public void testQuantifiedComparisonPredicateInteger() {

    final List<Object> values = Arrays.asList(new Object[] { 1, 2, 3 });
    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_INT, values);

    testQuantifiedComparisonPredicate(props, values, PREDICATE_QUANTIFIED_QUERY_TEMPLATE_INTEGER,
      TEST_CMIS_PROPERY_MULTIPLE_INT);
  }

  @Test
  public void testQuantifiedComparisonPredicateBoolean() {

    final List<Object> values = Arrays.asList(new Object[] { true, false });
    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_BOOLEAN, values);

    testQuantifiedComparisonPredicate(props, values, PREDICATE_QUANTIFIED_QUERY_TEMPLATE_BOOLEAN,
      TEST_CMIS_PROPERY_MULTIPLE_BOOLEAN);
  }

  @Test
  public void testQuantifiedComparisonPredicateDouble() {

    final List<Object> values = Arrays.asList(new Object[] { 1.2, 3.1 });
    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_DOUBLE, values);

    testQuantifiedComparisonPredicate(props, values, PREDICATE_QUANTIFIED_QUERY_TEMPLATE_DECIMAL,
      TEST_CMIS_PROPERY_MULTIPLE_DOUBLE);
  }

  @Test
  public void testQuantifiedComparisonPredicateDateTime() {

    final List<GregorianCalendar> storeValues = new ArrayList<GregorianCalendar>();
    final List<Object> searchValues = new ArrayList<Object>();

    final GregorianCalendar cal = new GregorianCalendar();
    storeValues.add((GregorianCalendar)cal.clone());
    searchValues.add(ISO8601DateFormat.format(cal.getTime()));
    cal.add(Calendar.MILLISECOND, 1);
    storeValues.add((GregorianCalendar)cal.clone());
    searchValues.add(ISO8601DateFormat.format(cal.getTime()));

    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_DATE_TIME, storeValues);

    testQuantifiedComparisonPredicate(props, searchValues,
      PREDICATE_QUANTIFIED_QUERY_TEMPLATE_DATETIME,
      TEST_CMIS_PROPERY_MULTIPLE_DATE_TIME);
  }

  /**
   * The quantified comparison predicate only applies to multi-valued properties: it can not be used for single valued properties. Only the equality
   * operator is supported. The only quantifier supported is ANY (ALL and SOME are not supported).
   * 
   * @param props
   *          - properties to add to the created test document
   * @param testValues
   *          - a list of values which will all be queried for
   * @param queryTemplate
   *          - The name of the quey template
   * @param propertyName
   *          - The property name being tested
   * 
   */
  private void testQuantifiedComparisonPredicate(final Map<String, Object> props,
    final List<Object> testValues, final String queryTemplate, final String propertyName) {

    final String expectedId = createTestCMISDocument(testRootFolder, "test", props).getId();
    for (final Object value: testValues) {
      assertQueryResult(queryTemplate, expectedId,
        testRootFolder.getId(),
        value, Predicate.QUANTIFIED_COMPARISION.getSymbol(), propertyName);
    }
  }

  @Test
  public void testQuantifiedInPredicateString() {

    final List<Object> propertyValues = new ArrayList<Object>();
    propertyValues.add("foo");
    propertyValues.add("bar");
    propertyValues.add("baz");

    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_STRING, propertyValues);

    final Set<Object> searchValues = new HashSet<Object>();
    searchValues.addAll(propertyValues);
    // Note we add a value that isn't part of the property as the predicate should match if ANY values match not ALL.
    searchValues.add("springy");

    testQuantifiedInPredicate(props, bracketAndDelimit(searchValues, true),
      TEST_CMIS_PROPERY_MULTIPLE_STRING);
  }

  @Test
  public void testQuantifiedInPredicateInteger() {

    final List<Object> propertyValues = new ArrayList<Object>();
    propertyValues.add(1);
    propertyValues.add(2);
    propertyValues.add(3);

    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_INT, propertyValues);

    final Set<Object> searchValues = new HashSet<Object>();
    searchValues.addAll(propertyValues);
    // Note we add a value that isn't part of the property as the predicate should match if ANY values match not ALL.
    searchValues.add(99);

    testQuantifiedInPredicate(props, bracketAndDelimit(searchValues, false),
      TEST_CMIS_PROPERY_MULTIPLE_INT);
  }

  @Test
  public void testQuantifiedInPredicateDouble() {

    final List<Object> propertyValues = new ArrayList<Object>();
    propertyValues.add(1.1);
    propertyValues.add(2.3);
    propertyValues.add(3.1);

    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_DOUBLE, propertyValues);

    final Set<Object> searchValues = new HashSet<Object>();
    searchValues.addAll(propertyValues);
    // Note we add a value that isn't part of the property as the predicate should match if ANY values match not ALL.
    searchValues.add(99.2);

    testQuantifiedInPredicate(props, bracketAndDelimit(searchValues, false),
      TEST_CMIS_PROPERY_MULTIPLE_DOUBLE);
  }

  @Test
  public void testQuantifiedInPredicateBoolean() {

    final List<Object> propertyValues = new ArrayList<Object>();
    propertyValues.add(true);

    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_BOOLEAN, propertyValues);

    final Set<Object> searchValues = new HashSet<Object>();
    searchValues.addAll(propertyValues);
    // Note we add a value that isn't part of the property as the predicate should match if ANY values match not ALL.
    searchValues.add(false);
    searchValues.add("FALSE");

    testQuantifiedInPredicate(props, bracketAndDelimit(searchValues, false),
      TEST_CMIS_PROPERY_MULTIPLE_BOOLEAN);
  }

  @Test
  public void testQuantifiedInPredicateDateTime() {

    final List<Object> propertyValues = new ArrayList<Object>();
    final GregorianCalendar cal = new GregorianCalendar();
    propertyValues.add(cal.clone());
    cal.add(Calendar.YEAR, 1);
    propertyValues.add(cal.clone());

    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(TEST_CMIS_PROPERY_MULTIPLE_DATE_TIME, propertyValues);

    final Set<Object> searchValues = new HashSet<Object>();
    for (final Object object: propertyValues) {
      searchValues.add(ISO8601DateFormat.format(((GregorianCalendar)object).getTime()));
    }

    // Note we add a value that isn't part of the property as the predicate should match if ANY values match not ALL.
    cal.add(Calendar.YEAR, 50);
    searchValues.add((ISO8601DateFormat.format(cal.getTime())));

    testQuantifiedInPredicate(props, bracketAndDelimit(searchValues, true),
      TEST_CMIS_PROPERY_MULTIPLE_DATE_TIME);
  }

  @Test
  public void testContainsPredicate() {
    final Map<String, Object> props = new HashMap<String, Object>();
    final String testDocumentId = createTestCMISDocument(testRootFolder, "test", props, "test")
      .getId();
    final String tubeDocumentId = createTestCMISDocument(testRootFolder, "tube", props, "tube")
      .getId();
    final String testTubeDocumentId = createTestCMISDocument(testRootFolder, "testTube", props,
      "test tube").getId();
    final String tubeTestDocumentId = createTestCMISDocument(testRootFolder, "tubeTest", props,
      "tube test").getId();

    // term
    Set<String> expectedIds = toSet(testDocumentId, testTubeDocumentId, tubeTestDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContains("test"));

    // term AND (default)
    expectedIds = toSet(testTubeDocumentId, tubeTestDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContains("test tube"));

    // term OR
    expectedIds = toSet(testDocumentId, tubeDocumentId, testTubeDocumentId, tubeTestDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContains("test OR tube"));

    // negation precedence over OR - Note This does NOT mean find all documents without "test" and then from those find all those with "tube"
    // compare with bracketed negation OR terms below
    expectedIds = toSet(tubeDocumentId, testTubeDocumentId, tubeTestDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContains("-test OR tube"));

    // bracketed negation OR terms,
    expectedIds = Collections.emptySet();
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContains("-(test OR tube)"));

    // Phrase
    expectedIds = toSet(testTubeDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContainsPhrase("test tube"));

    // negated term
    expectedIds = toSet(tubeDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContains("-test"));

    // negated phrase
    expectedIds = toSet(testDocumentId, tubeDocumentId, tubeTestDocumentId);
    assertQueryResults(SINGLE_VAL_PREDICATE_QUERY_TEMPLATE_STRING, expectedIds,
      testRootFolder.getId(), buildContainsNegatedPhrase("test tube"));
  }

  @Test
  public void folderSearches() {

    final String allFoldersInFolderQuery = "SELECT * FROM cmis:folder WHERE in_folder('%s') and cmis:name='%s'";
    final String allFoldersInTreeQuery = "SELECT * FROM cmis:folder WHERE in_tree('%s') and cmis:name='%s'";

    // Create two folders of the same name one beneath the other
    final String testFolderName = "my_test_folder";
    final Folder testFolder = createTestCMISFolder(testRootFolder, testFolderName);
    final Folder testSubFolder = createTestCMISFolder(testFolder, testFolderName);


    // Search IMMEADIATELY within the test root space using in_folder
    final String expectedId = testFolder.getId();
    assertQueryResult(allFoldersInFolderQuery, expectedId, testRootFolder.getId(), testFolderName);

    // Now search again this time limit the search to ANYWHERE beneath the test
    // root space using in_tree
    final Set<String> expectedIds = toSet(testFolder.getId(), testSubFolder.getId());
    assertQueryResults(allFoldersInTreeQuery, expectedIds, testRootFolder.getId(), testFolderName);
  }

  /**
   * START OF TESTS FOR ALFRESCO OPEN CMIS EXTENSIONS
   * 
   */

  @Test
  public void createWithAspect() {
    final Map<String, Object> props = new HashMap<String, Object>();
    final String description = "test description";

    final String typeAndAspect = documentPrefix(TEST_CMIS_DOCUMENT_TYPE) + ","
      + aspectPrefix(ASPECT_TITLED);

    props.put(PropertyIds.OBJECT_TYPE_ID, typeAndAspect);
    props.put(PROPERTY_DESCRIPTION, description);

    final String expectedId = createTestCMISDocument(testRootFolder, "test", props).getId();

    final String query = "select d.*, t.* from  swct:document as d join cm:titled as t on d.cmis:objectid = t.cmis:objectid where t.cm:description = '%s'";
    assertQueryResults(String.format(query, description), false, expectedId);
  }

  @Test
  public void addRemoveAspects() {
    final String description = "test description";
    final String queryTemplate = "select d.*, t.* from  swct:document as d join cm:titled as t on d.cmis:objectid = t.cmis:objectid where t.cm:description = '%s'";
    final String query = String.format(queryTemplate, description);

    final AlfrescoDocument alfDoc = (AlfrescoDocument)createTestCMISDocument(testRootFolder,
      "test", null);

    final Set<String> emptyIdSet = Collections.emptySet();
    assertQueryResults(query, false, emptyIdSet);

    // Add the titled aspect and set the description property
    final Map<String, Object> props = new HashMap<String, Object>();
    props.put(PROPERTY_DESCRIPTION, description);
    final String prefixedAspect = aspectPrefix(ASPECT_TITLED);
    alfDoc.addAspect(prefixedAspect, props);
    assertQueryResults(query, false, alfDoc.getId());

    // Now remove the aspect
    alfDoc.removeAspect(prefixedAspect);
    assertQueryResults(query, false, emptyIdSet);
  }

  /**
   * END OF TESTS FOR ALFRESCO OPEN CMIS EXTENSION
   * 
   */

  private String buildContains(final String s) {
    return buildContains(s, false, false);
  }

  private String buildContainsPhrase(final String s) {
    return buildContains(s, true, false);
  }

  private String buildContainsNegatedPhrase(final String s) {
    return buildContains(s, true, true);
  }

  private String buildContains(final String s, final boolean isPhrase, final boolean negate) {
    final StringBuilder sb = new StringBuilder(Predicate.CONTAINS.getSymbol()).append("('");
    sb.append(isPhrase ? escape(s, negate) : s);
    sb.append("')");
    return sb.toString();
  }

  private String escape(final String s, final boolean negate) {
    final StringBuilder sb = new StringBuilder();
    if (negate) {
      sb.append("-");
    }
    sb.append("\\'").append(s).append("\\'");
    return sb.toString();
  }

  private Set<String> toSet(final String... strings) {
    final Set<String> setOfStrings = new HashSet<String>(strings.length);
    setOfStrings.addAll(Arrays.asList(strings));
    return setOfStrings;
  }

  /**
   * This is an extension to SQL-92 and defines a new IN predicate for use only with multi-valued properties. The only qualifier support is ANY. The
   * predicate is true if any of the values of a multi-valued property match any of those in the IN list. The quantified IN predicate is only
   * supported for multi-valued properties of types with a data type that supports IN, as described above.
   * 
   * @param props
   *          - properties to add to the created test document
   * @param predicateValues
   *          - Bracketed, Comma separated list of values to use for the query. The calling function should ensure that this list is not EXACTLY the
   *          same as the values stored in the multi-value property as we are testing for ANY
   * @param propertyName
   *          - The name of the property being tested
   */
  private void testQuantifiedInPredicate(final Map<String, Object> props,
    final String predicateValues, final String propertyName) {

    final String expectedId = createTestCMISDocument(testRootFolder, "test", props).getId();
    assertQueryResult(PREDICATE_QUANTIFIED_IN_TEMPLATE_STRING, expectedId,
        testRootFolder.getId(), propertyName, predicateValues);
  }

  /**
   * 
   * @param values
   *          e.g {"bar", "baz"}
   * @param quoteElements
   *          - If true than each element will be quoted with single quotes.
   * @return e.g ('bar','baz')
   */
  private String bracketAndDelimit(final Set<Object> values, final boolean quoteElements) {
    String separatedValues = "";
    if (quoteElements) {
      separatedValues = StringUtils.join(quoteValues(values).toArray(), ",");
    } else {
      separatedValues = StringUtils.join(values.toArray(), ",");
    }
    final StringBuilder sb = new StringBuilder("(");
    sb.append(separatedValues);
    sb.append(")");
    return sb.toString();
  }

  /**
   * 
   * @param values
   *          eq {"foo", "bar"}
   * @return {"'foo'", "'bar'"}
   */
  private Set<Object> quoteValues(final Set<Object> values) {
    final Set<Object> quotedValues = new HashSet<Object>(values.size());
    for (final Object value: values) {
      quotedValues.add(quote(value));
    }
    return quotedValues;
  }

  /**
   * 
   * @param s
   *          e.g baz
   * @return 'baz'
   */
  private String quote(final Object o) {
    return new StringBuilder("'").append(o).append("'").toString();
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

  private void testInPredicate(final Set<Object> allTokens, final String propertyName,
    final boolean quoteElements) {

    // Make searchTokens a subset of allTokens by removing the last element
    final TreeSet<Object> searchTokens = new TreeSet<Object>();
    searchTokens.addAll(allTokens);
    searchTokens.remove(searchTokens.last());

    // These are the string that will be added to the IN predicate
    final Set<String> expectedIds = new HashSet<String>();

    // Create test documents
    final Map<String, Object> props = new HashMap<String, Object>();

    int counter = 0;
    for (final Object object: allTokens) {
      props.put(propertyName, object);
      final Document document = createTestCMISDocument(testRootFolder, "test" + counter++, props);
      if (searchTokens.contains(object)) {
        expectedIds.add(document.getId());
      }
    }

    testInPredicateValues(searchTokens, expectedIds, propertyName, quoteElements);
  }

  /**
   * @param propertyName
   * @param quoteElements
   * @param searchTokens
   * @param expectedIds
   */
  private void testInPredicateValues(final TreeSet<Object> searchTokens,
    final Set<String> expectedIds, final String propertyName, final boolean quoteElements) {

    final ItemIterable<QueryResult> predicateQueryResults = getPredicateQueryResults(
      PREDICATE_QUERY_TEMPLATE_UNQUOTED_STRING, 1, testRootFolder.getId(),
      propertyName,
      Predicate.IN.getSymbol(), bracketAndDelimit(searchTokens, quoteElements));

    // Compare the expected string with the actual strings
    final Set<String> actualIds = new HashSet<String>();

    for (final QueryResult queryResult: predicateQueryResults) {
      actualIds.add((String)queryResult.getPropertyById(PropertyIds.OBJECT_ID).getFirstValue());
    }

    assertEquals(expectedIds, actualIds);
  }

  /**
   * @param queryTemplate
   *          - The name of a query template
   * @param expectedId
   *          - The object id of the single result expected
   * @param values
   *          - The values to replace in the template.
   */
  private void assertQueryResult(final String queryTemplate,
    final String expectedId, final Object... values) {

    assertQueryResults(queryTemplate, toSet(expectedId), values);
  }

  /**
   * @param queryTemplate
   *          - The name of a query template
   * @param expectedIds
   *          - The object ids of the expected results
   * @param values
   *          - The values to replace in the template.
   */
  private void assertQueryResults(final String queryTemplate,
    final Set<String> expectedIds, final Object... values) {

    assertQueryResults(String.format(queryTemplate, values), false, expectedIds);
  }

  private void assertQueryResults(final String query, final boolean searchAllVersions,
    final String... expectedIds) {

    assertQueryResults(query, searchAllVersions, toSet(expectedIds));
  }

  /**
   * 
   * @param query
   * @param searchAllVersions
   * @param expectedIds
   *          - The
   */
  private void assertQueryResults(final String query, final boolean searchAllVersions,
    final Set<String> expectedIds) {

    final Set<String> actualIds = new HashSet<String>();

    final ItemIterable<QueryResult> results = executeQuery(
      query, false);

    assertEquals("Wrong result count", expectedIds.size(), results.getTotalNumItems());
    for (final QueryResult result: results) {
      actualIds.add((String)result.getPropertyValueById(PropertyIds.OBJECT_ID));
    }
    assertEquals("Result ids do not match expected values", expectedIds, actualIds);
  }


  private ItemIterable<QueryResult> getPredicateQueryResults(final String queryTemplate,
    final int expectedResultCount,
    final Object... values) {

    return executeQuery(String.format(queryTemplate, values), false);
  }

  /**
   * 
   * @param query
   * @param searchAllVersions
   * @return The query results
   */
  private ItemIterable<QueryResult> executeQuery(final String query, final boolean searchAllVersions) {
    System.out.println("Executing query " + query + " (Searching all versions: "
      + searchAllVersions + ")");
    return session.query(query, searchAllVersions);
  }

  private Document createTestCMISDocument(final Folder parent, final String name,
    final Map<String, Object> props) {

    return createTestCMISDocument(parent, name, props, null);
  }

  /**
   * Create a new child folder of type swct:folder
   * 
   * @param parent
   *          The parent folder
   * @param name
   *          The name of the new folder
   * @return
   */
  private Folder createTestCMISFolder(final Folder parent, final String name) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(PropertyIds.NAME, name);
    props.put(PropertyIds.OBJECT_TYPE_ID, folderPrefix(TEST_CMIS_FOLDER_TYPE));
    return parent.createFolder(props);
  }


  private Document createTestCMISDocument(final Folder parent, final String name,
    final Map<String, Object> props, final String content) {

    final Map<String, Object> properties = new HashMap<String, Object>();
    if (props != null) {
      properties.putAll(props);
    }
    ContentStream contentStream = null;
    if (content != null) {
      contentStream = new ContentStreamImpl("test", "text/plain", content);
    }
    properties.put(PropertyIds.NAME, name);
    if (!properties.containsKey(PropertyIds.OBJECT_TYPE_ID)) {
      properties.put(PropertyIds.OBJECT_TYPE_ID, documentPrefix(TEST_CMIS_DOCUMENT_TYPE));
    }
    return parent.createDocument(properties, contentStream, null);
  }

  /**
   * Prefix a Document type for CMIS
   * 
   * @param type
   *          e.g my:type
   * @return D:my:type
   */
  private String documentPrefix(final String documentType) {
    return "D:" + documentType;
  }

  /**
   * Prefix a Folder type for CMIS
   * 
   * @param type
   *          e.g my:type
   * @return F:my:type
   */
  private String folderPrefix(final String folderType) {
    return "F:" + folderType;
  }

  /**
   * Prefix an Aspect type for CMIS
   * 
   * @param type
   *          e.g my:type
   * @return P:my:type
   */
  private String aspectPrefix(final String aspectType) {
    return "P:" + aspectType;
  }
}