/**
 * 
 */
package com.springyweb.alfresco.tests.cmis;

/**
 * @author si
 * 
 */
public enum Predicate {

  EQUALS("="),
  NOT_EQUALS("<>"),
  GREATER_THAN(">"),
  GREATER_THAN_EQUAL_TO(">="),
  LESS_THAN("<"),
  LESS_THAN_EQUAL_TO("<="),
  IN("IN"),
  LIKE("LIKE"),
  IS_NOT_NULL("IS NOT NULL"),
  IS_NULL("IS NULL");

  private String symbol;

  private Predicate(final String symbol) {
    this.symbol = symbol;
  }

  /**
   * @return the symbol
   */
  public String getSymbol() {
    return symbol;
  }
}
