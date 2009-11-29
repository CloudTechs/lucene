package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.text.Collator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermRef;
import org.apache.lucene.index.Terms;
//import org.apache.lucene.index.Term;
import org.apache.lucene.util.StringHelper;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified range parameters.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */
public class TermRangeTermsEnum extends FilteredTermsEnum {

  private Collator collator;
  private String field;
  private String upperTermText;
  private String lowerTermText;
  private boolean includeLower;
  private boolean includeUpper;
  final private TermRef lowerTermRef;
  final private TermRef upperTermRef;
  private final boolean empty;
  private final TermRef.Comparator termComp;

  /**
   * Enumerates all terms greater/equal than <code>lowerTerm</code>
   * but less/equal than <code>upperTerm</code>. 
   * 
   * If an endpoint is null, it is said to be "open". Either or both 
   * endpoints may be open.  Open endpoints may not be exclusive 
   * (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
   * 
   * @param reader
   * @param field
   *          An interned field that holds both lower and upper terms.
   * @param lowerTermText
   *          The term text at the lower end of the range
   * @param upperTermText
   *          The term text at the upper end of the range
   * @param includeLower
   *          If true, the <code>lowerTerm</code> is included in the range.
   * @param includeUpper
   *          If true, the <code>upperTerm</code> is included in the range.
   * @param collator
   *          The collator to use to collate index Terms, to determine their
   *          membership in the range bounded by <code>lowerTerm</code> and
   *          <code>upperTerm</code>.
   * 
   * @throws IOException
   */
  public TermRangeTermsEnum(IndexReader reader, String field, String lowerTermText, String upperTermText, 
    boolean includeLower, boolean includeUpper, Collator collator) throws IOException {
    this.collator = collator;
    this.upperTermText = upperTermText;
    this.lowerTermText = lowerTermText;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.field = StringHelper.intern(field);

    // do a little bit of normalization...
    // open ended range queries should always be inclusive.
    if (this.lowerTermText == null) {
      this.lowerTermText = "";
      this.includeLower = true;
    }
    lowerTermRef = new TermRef(this.lowerTermText);

    if (this.upperTermText == null) {
      this.includeUpper = true;
      upperTermRef = null;
    } else {
      upperTermRef = new TermRef(upperTermText);
    }

    String startTermText = collator == null ? this.lowerTermText : "";
    Terms terms = reader.fields().terms(field);

    if (terms != null) {
      termComp = terms.getTermComparator();
      final boolean foundFirstTerm = setEnum(terms.iterator(), new TermRef(startTermText)) != null;

      if (foundFirstTerm && collator == null && !this.includeLower && term().termEquals(lowerTermRef)) {
        empty = next() == null;
      } else {
        empty = !foundFirstTerm;
      }
    } else {
      empty = true;
      termComp = null;
    }
  }

  @Override
  public float difference() {
    return 1.0f;
  }

  @Override
  public boolean empty() {
    return empty;
  }

  @Override
  public String field() {
    return field;
  }

  @Override
  protected AcceptStatus accept(TermRef term) {
    if (collator == null) {
      // Use this field's default sort ordering
      if (upperTermRef != null) {
        final int cmp = termComp.compare(upperTermRef, term);
        /*
         * if beyond the upper term, or is exclusive and this is equal to
         * the upper term, break out
         */
        if ((cmp < 0) ||
            (!includeUpper && cmp==0)) {
          return AcceptStatus.END;
        }
      }
      return AcceptStatus.YES;
    } else {
      if ((includeLower
           ? collator.compare(term.toString(), lowerTermText) >= 0
           : collator.compare(term.toString(), lowerTermText) > 0)
          && (upperTermText == null
              || (includeUpper
                  ? collator.compare(term.toString(), upperTermText) <= 0
                  : collator.compare(term.toString(), upperTermText) < 0))) {
        return AcceptStatus.YES;
      }
      return AcceptStatus.NO;
    }
  }
}
