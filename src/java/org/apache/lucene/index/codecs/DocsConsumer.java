package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.DocsEnum;

/**
 * NOTE: this API is experimental and will likely change
 */

public abstract class DocsConsumer {

  // nocommit
  public String desc;
  /*
  public boolean setDesc(String desc) {
    this.desc = desc;
    return true;
  }
  */

  /** Adds a new doc in this term.  Return null if this
   *  consumer doesn't need to see the positions for this
   *  doc. */
  public abstract PositionsConsumer addDoc(int docID, int termDocFreq) throws IOException;

  public static class DocsMergeState {
    DocsEnum docsEnum;
    int[] docMap;
    int docBase;
  }

  /** Default merge impl: append documents, mapping around
   *  deletes */
  public int merge(MergeState mergeState, DocsMergeState[] toMerge, int count) throws IOException {

    int df = 0;
    // Append docs in order:
    for(int i=0;i<count;i++) {
      final DocsEnum docs = toMerge[i].docsEnum;
      final int[] docMap = toMerge[i].docMap;
      final int base = toMerge[i].docBase;

      while(true) {
        final int startDoc = docs.next();
        if (startDoc == DocsEnum.NO_MORE_DOCS) {
          break;
        }
        df++;

        int doc;
        if (docMap != null) {
          // map around deletions
          doc = docMap[startDoc];
          assert doc != -1: "postings enum returned deleted docID " + startDoc + " freq=" + docs.freq() + " df=" + df;
        } else {
          doc = startDoc;
        }

        doc += base;                              // convert to merged space
        assert doc < mergeState.mergedDocCount: "doc=" + doc + " maxDoc=" + mergeState.mergedDocCount;

        final int freq = docs.freq();
        final PositionsConsumer posConsumer = addDoc(doc, freq);

        // nocommit -- omitTF should be "private", and this
        // code (and FreqProxTermsWriter) should instead
        // check if posConsumer is null?
        if (!mergeState.omitTermFreqAndPositions) {
          posConsumer.merge(mergeState, docs.positions(), freq);
        }
      }
    }

    return df;
  }
}
