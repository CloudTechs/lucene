package org.apache.lucene.index;

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

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;

/**
 * NOTE: this API is experimental and will likely change
 */

/** Iterator to seek ({@link #seek}) or step through ({@link
 * #next} terms, obtain frequency information ({@link
 * #docFreq}), and obtain a {@link DocsEnum} for the current
 * term ({@link #docs)}.
 * 
 * <p>On obtaining a TermsEnum, you must first call
 * {@link #next} or {@link #seek}. */
public abstract class TermsEnum extends AttributeSource {

  /** Represents returned result from {@link TermsEnum.seek}.
   *  If status is FOUND, then the precise term was found.
   *  If status is NOT_FOUND, then a different term was
   *  found.  If the status is END, the end of the iteration
   *  was hit. */
  public static enum SeekStatus {END, FOUND, NOT_FOUND};

  /** Seeks to the specified term.  Returns SeekResult to
   *  indicate whether exact term was found, a different
   *  term was found, or EOF was hit.  The target term may
   *  be befor or after the current term. */
  public abstract SeekStatus seek(TermRef text) throws IOException;

  /** Seeks to the specified term by ordinal (position) as
   *  previously returned by {@link #ord}.  The target ord
   *  may be befor or after the current ord.  See {@link
   *  #seek(TermRef). */
  public abstract SeekStatus seek(long ord) throws IOException;
  
  /** Increments the enumeration to the next element.
   *  Returns the resulting TermRef, or null if the end was
   *  hit.  The returned TermRef may be re-used across calls
   *  to next. */
  public abstract TermRef next() throws IOException;

  /** Returns current term.  This is undefined after next()
   *  returns null or seek returns {@link SeekStatus#END}. */
  public abstract TermRef term() throws IOException;

  /** Returns ordinal position for current term.  Not all
   *  codecs implement this, so be prepared to catch an
   *  {@link UnsupportedOperationException}.  This is
   *  undefined after next() returns null or seek returns
   *  {@link SeekStatus#END}. */
  public abstract long ord() throws IOException;

  /** Returns the docFreq of the current term.  This is
   *  undefined after next() returns null or seek returns
   *  {@link SeekStatus#END}.*/
  public abstract int docFreq();

  // nocommit -- clarify if this may return null
  /** Get {@link DocsEnum} for the current term.  The
   *  returned {@link DocsEnum} may share state with this
   *  TermsEnum instance, so you should not call this
   *  TermsEnum's {@link #seek} or {@link #next} until you
   *  are done using the DocsEnum. */
  public abstract DocsEnum docs(Bits skipDocs) throws IOException;

  /** Return the TermRef Comparator used to sort terms
   *  provided by the iterator.  NOTE: this may return null
   *  if there are no terms.  This method may be invoked
   *  many times; it's best to cache a single instance &
   *  reuse it. */
  public abstract TermRef.Comparator getTermComparator() throws IOException;
}
