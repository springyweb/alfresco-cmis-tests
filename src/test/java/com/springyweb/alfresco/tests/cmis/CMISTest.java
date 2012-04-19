package com.springyweb.alfresco.tests.cmis;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
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

  // Note the replaceable parameters here are (in order) folder id,property,predicate,value
  // SELECT * from swct:document where in_folder('workspace://SpacesStore/c22f856c-6cec-4e16-9c1c-60df621bba16') and swct:propSingleBoolean IS NULL
  private static final String NULL_PREDICATE_QUERY_TEMPLATE_STRING = "SELECT * from "
    + TEST_CMIS_DOCUMENT_TYPE + " where in_folder('%s') and %s %s";

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
  public void comparisonPredicatesString() {

    // Create test documents with a single string property
    final Map<String, Object> props = new HashMap<String, Object>();

    final String[] strings = { "b", "Bc", "c", "Cb" };
    for (int i = 0; i < strings.length; i++) {
      props.put(TEST_CMIS_PROPERY_SINGLE_STRING, strings[i]);
      createTestCMISDocument(testRootFolder, "test" + i, props);
    }

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 1, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.EQUALS.getSymbol(), "b");

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.NOT_EQUALS.getSymbol(), "b");

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 2, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.LESS_THAN.getSymbol(), "c");

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), "c");

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.GREATER_THAN.getSymbol(), "b");

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 4, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_STRING,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), "b");
  }

  @Test
  public void comparisonPredicatesInteger() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 4 test documents
    for (int i = 1; i <= 4; i++) {
      props.put(TEST_CMIS_PROPERY_SINGLE_INT, i);
      createTestCMISDocument(testRootFolder, "test" + i, props);
    }

    testPredicate(PREDICATE_QUERY_TEMPLATE_INTEGER, 1, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.EQUALS.getSymbol(), 1);

    testPredicate(PREDICATE_QUERY_TEMPLATE_INTEGER, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.NOT_EQUALS.getSymbol(), 1);

    testPredicate(PREDICATE_QUERY_TEMPLATE_INTEGER, 2, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.LESS_THAN.getSymbol(), 3);

    testPredicate(PREDICATE_QUERY_TEMPLATE_INTEGER, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), 3);

    testPredicate(PREDICATE_QUERY_TEMPLATE_INTEGER, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.GREATER_THAN.getSymbol(), 1);

    testPredicate(PREDICATE_QUERY_TEMPLATE_INTEGER, 4, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_INT,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), 1);

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

    testPredicate(PREDICATE_QUERY_TEMPLATE_DECIMAL, 1, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.EQUALS.getSymbol(), 1.0);

    testPredicate(PREDICATE_QUERY_TEMPLATE_DECIMAL, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.NOT_EQUALS.getSymbol(), 1.0);

    testPredicate(PREDICATE_QUERY_TEMPLATE_DECIMAL, 2, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.LESS_THAN.getSymbol(), 1.2);

    testPredicate(PREDICATE_QUERY_TEMPLATE_DECIMAL, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.LESS_THAN_EQUAL_TO.getSymbol(), 1.2);

    testPredicate(PREDICATE_QUERY_TEMPLATE_DECIMAL, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.GREATER_THAN.getSymbol(), 1.0);

    testPredicate(PREDICATE_QUERY_TEMPLATE_DECIMAL, 4, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DOUBLE,
      Predicate.GREATER_THAN_EQUAL_TO.getSymbol(), 1.0);
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
      testPredicate(PREDICATE_QUERY_TEMPLATE_BOOLEAN, 1, testRootFolder.getId(),
        TEST_CMIS_PROPERY_SINGLE_BOOLEAN,
        Predicate.EQUALS.getSymbol(), val);
    }
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
    final GregorianCalendar calendar = new GregorianCalendar();

    for (int i = 0; i < dates.length; i++) {
      props.clear();
      dates[i] = calendar.getTime();
      // Note: For OpenCmis to work we add the calendar object not the date (see
      // http://blog.remysaissy.com/2012/04/solving-property-foo-is-datetime.html)
      props.put(TEST_CMIS_PROPERY_SINGLE_DATE_TIME, calendar);
      createTestCMISDocument(testRootFolder, "test" + i, props);
      calendar.add(Calendar.MILLISECOND, 1);
    }

    testPredicate(PREDICATE_QUERY_TEMPLATE_DATETIME, 1, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.EQUALS.getSymbol(), ISO8601DateFormat.format(dates[0]));

    testPredicate(PREDICATE_QUERY_TEMPLATE_DATETIME, 3, testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_DATE_TIME,
      Predicate.NOT_EQUALS.getSymbol(), ISO8601DateFormat.format(dates[0]));

    // TODO: Add date inequality tests
  }

  @Test
  public void comparisonPredicatesId() {

    final Map<String, Object> props = new HashMap<String, Object>();
    // Create 3 test docs - no properties required for ID testing
    final String id1 = createTestCMISDocument(testRootFolder, "test1", props).getId();
    createTestCMISDocument(testRootFolder, "test2", props).getId();
    createTestCMISDocument(testRootFolder, "test3", props).getId();

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 1, testRootFolder.getId(),
      PropertyIds.OBJECT_ID,
      Predicate.EQUALS.getSymbol(), id1);

    testPredicate(PREDICATE_QUERY_TEMPLATE_STRING, 2, testRootFolder.getId(),
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
      testPredicateQuerySingleResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId,
        testRootFolder.getId(),
        PropertyIds.NAME, Predicate.LIKE.getSymbol(), testVal);
    }

    // Test escaping %
    expectedId = createTestCMISDocument(testRootFolder, "t%t", props).getId();
    testPredicateQuerySingleResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId,
      testRootFolder.getId(),
      PropertyIds.NAME, Predicate.LIKE.getSymbol(), "t\\%t");

    // Test escaping _
    expectedId = createTestCMISDocument(testRootFolder, "t__t", props).getId();
    testPredicateQuerySingleResult(PREDICATE_QUERY_TEMPLATE_STRING, expectedId,
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
    testPredicateQuerySingleResult(NULL_PREDICATE_QUERY_TEMPLATE_STRING, idWithoutPropertySet,
      testRootFolder.getId(),
      TEST_CMIS_PROPERY_SINGLE_BOOLEAN, Predicate.IS_NULL.getSymbol());

    // Test for the item with the property set using IS NOT NULL
    testPredicateQuerySingleResult(NULL_PREDICATE_QUERY_TEMPLATE_STRING, idWithPropertySet,
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
      testPredicateQuerySingleResult(queryTemplate, expectedId,
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

  private void testQuantifiedInPredicate(final Map<String, Object> props,
    final String predicateValues, final String propertyName) {

    final String expectedId = createTestCMISDocument(testRootFolder, "test", props).getId();
    testPredicateQuerySingleResult(PREDICATE_QUANTIFIED_IN_TEMPLATE_STRING, expectedId,
        testRootFolder.getId(), propertyName, predicateValues);
  }

  /**
   * 
   * @param values
   *          e.g {"bar", "baz"}
   * @return ('bar','baz')
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
   * Helper method to test predicate queries
   * 
   * @param queryTemplate
   *          - A query with parameters which are resolvable via String.format
   * 
   * @param expectedResultCount
   * @param values
   */
  private void testPredicate(final String queryTemplate, final int expectedResultCount,
    final Object... values) {

    assertEquals("Wrong result count", expectedResultCount,
      queryResultCount(String.format(queryTemplate, values), false));
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

  private void testPredicateQuerySingleResult(final String queryTemplate,
    final String expectedId, final Object... values) {

    final ItemIterable<QueryResult> results = executeQuery(
      String.format(queryTemplate, values), false);

    assertEquals("Wrong result count", 1, results.getTotalNumItems());
    final String actualId = (String)results.iterator().next()
      .getPropertyById(PropertyIds.OBJECT_ID).getFirstValue();
    assertEquals("Wrong id found", expectedId, actualId);

  }

  private ItemIterable<QueryResult> getPredicateQueryResults(final String queryTemplate,
    final int expectedResultCount,
    final Object... values) {

    return executeQuery(String.format(queryTemplate, values), false);
  }

  /**
   * @param query
   * @param searchAllVersions
   * @return The total number of items found by the query
   */
  private long queryResultCount(final String query, final boolean searchAllVersions) {
    return executeQuery(query, searchAllVersions).getTotalNumItems();
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
    return session.query(query, false);
  }

  private Document createTestCMISDocument(final Folder parent, final String name,
    final Map<String, Object> props) {

    props.put(PropertyIds.NAME, name);
    props.put(PropertyIds.OBJECT_TYPE_ID, "D:" + TEST_CMIS_DOCUMENT_TYPE);
    return parent.createDocument(props, null, null);
  }
}