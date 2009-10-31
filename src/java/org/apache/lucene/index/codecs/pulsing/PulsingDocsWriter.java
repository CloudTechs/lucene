package org.apache.lucene.index.codecs.pulsing;

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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

import org.apache.lucene.index.codecs.DocsConsumer;
import org.apache.lucene.index.codecs.PositionsConsumer;
import org.apache.lucene.index.codecs.standard.StandardDocsConsumer;
import org.apache.lucene.index.codecs.standard.StandardPositionsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.Codec;

// TODO: we now pulse entirely according to docFreq of the
// term; it might be better to eg pulse by "net bytes used"
// so that a term that has only 1 doc but zillions of
// positions would not be inlined.  Though this is
// presumably rare in practice...

//nocommit: public 
public final class PulsingDocsWriter extends StandardDocsConsumer {

  final static String CODEC = "PulsedPostings";

  // To add a new version, increment from the last one, and
  // change VERSION_CURRENT to point to your new version:
  final static int VERSION_START = 0;

  final static int VERSION_CURRENT = VERSION_START;

  IndexOutput termsOut;

  boolean omitTF;
  boolean storePayloads;

  // Starts a new term
  FieldInfo fieldInfo;

  // nocommit
  String desc;

  // nocommit: public
  public static class Document {
    int docID;
    int termDocFreq;
    int numPositions;
    Position[] positions;
    Document() {
      positions = new Position[1];
      positions[0] = new Position();
    }
    
    @Override
    public Object clone() {
      Document doc = new Document();
      doc.docID = docID;
      doc.termDocFreq = termDocFreq;
      doc.numPositions = numPositions;
      doc.positions = new Position[positions.length];
      for(int i = 0; i < positions.length; i++) {
        doc.positions[i] = (Position)positions[i].clone();
      }

      return doc;
    }

    void reallocPositions(int minSize) {
      final Position[] newArray = new Position[ArrayUtil.getNextSize(minSize)];
      System.arraycopy(positions, 0, newArray, 0, positions.length);
      for(int i=positions.length;i<newArray.length;i++)
        newArray[i] = new Position();
      positions = newArray;
    }
  }

  final Document[] pendingDocs;
  int pendingDocCount = 0;
  Document currentDoc;
  boolean pulsed;                                 // false if we've seen > maxPulsingDocFreq docs

  static class Position {
    byte[] payload;
    int pos;
    int payloadLength;
    
    @Override
    public Object clone() {
      Position position = new Position();
      position.pos = pos;
      position.payloadLength = payloadLength;
      if(payload != null) {
        position.payload = new byte[payload.length];
        System.arraycopy(payload, 0, position.payload, 0, payloadLength);
      }
      return position;
    }
  }

  // nocommit -- lazy init this?  ie, if every single term
  // was pulsed then we never need to use this fallback?
  // Fallback writer for non-pulsed terms:
  final StandardDocsConsumer wrappedDocsWriter;

  /** If docFreq <= maxPulsingDocFreq, its postings are
   *  inlined into terms dict */
  PulsingDocsWriter(SegmentWriteState state, int maxPulsingDocFreq, StandardDocsConsumer wrappedDocsWriter) throws IOException {
    super();

    pendingDocs = new Document[maxPulsingDocFreq];
    for(int i=0;i<maxPulsingDocFreq;i++) {
      pendingDocs[i] = new Document();
    }

    // We simply wrap another DocsConsumer, but only call on
    // it when doc freq is higher than our cutoff
    this.wrappedDocsWriter = wrappedDocsWriter;
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    Codec.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeVInt(pendingDocs.length);
    wrappedDocsWriter.start(termsOut);
  }

  @Override
  public void startTerm() {
    assert pendingDocCount == 0;
    pulsed = false;
  }

  // nocommit -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTF = fieldInfo.omitTermFreqAndPositions;
    storePayloads = fieldInfo.storePayloads;
    wrappedDocsWriter.setField(fieldInfo);
  }

  /** Simply buffers up positions */
  class PositionsWriter extends StandardPositionsConsumer {
    @Override
    public void start(IndexOutput termsOut) {}
    @Override
    public void startTerm() {}
    @Override
    public void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) {
      Position pos = currentDoc.positions[currentDoc.numPositions++];
      pos.pos = position;
      if (payload != null && payloadLength > 0) {
        if (pos.payload == null || payloadLength > pos.payload.length) {
          pos.payload = new byte[ArrayUtil.getNextSize(payloadLength)];
        }
        System.arraycopy(payload, payloadOffset, pos.payload, 0, payloadLength);
        pos.payloadLength = payloadLength;
      } else
        pos.payloadLength = 0;
    }
    @Override
    public void finishDoc() {
      assert currentDoc.numPositions == currentDoc.termDocFreq;
    }
    @Override
    public void finishTerm(boolean isIndexTerm) {}
    @Override
    public void close() {}
  }

  final PositionsWriter posWriter = new PositionsWriter();

  @Override
  public PositionsConsumer addDoc(int docID, int termDocFreq) throws IOException {

    assert docID >= 0: "got docID=" + docID;
        
    if (Codec.DEBUG)
      System.out.println("PW.addDoc: docID=" + docID + " pendingDocCount=" + pendingDocCount + " vs " + pendingDocs.length + " pulsed=" + pulsed);

    if (!pulsed && pendingDocCount == pendingDocs.length) {
      
      // OK we just crossed the threshold, this term should
      // now be written with our wrapped codec:
      wrappedDocsWriter.startTerm();
      
      if (Codec.DEBUG)
        System.out.println("  now flush buffer");

      // Flush all buffered docs
      for(int i=0;i<pendingDocCount;i++) {
        final Document doc = pendingDocs[i];
        if (Codec.DEBUG)
          System.out.println("  docID=" + doc.docID);

        PositionsConsumer posConsumer = wrappedDocsWriter.addDoc(doc.docID, doc.termDocFreq);
        if (!omitTF && posConsumer != null) {
          assert doc.termDocFreq == doc.numPositions;
          for(int j=0;j<doc.termDocFreq;j++) {
            final Position pos = doc.positions[j];
            if (pos.payload != null && pos.payloadLength > 0) {
              assert storePayloads;
              posConsumer.addPosition(pos.pos, pos.payload, 0, pos.payloadLength);
            } else
              posConsumer.addPosition(pos.pos, null, 0, 0);
          }
          posConsumer.finishDoc();
        }
      }

      pendingDocCount = 0;

      pulsed = true;
    }

    if (pulsed) {
      // We've already seen too many docs for this term --
      // just forward to our fallback writer
      return wrappedDocsWriter.addDoc(docID, termDocFreq);
    } else {
      currentDoc = pendingDocs[pendingDocCount++];
      currentDoc.docID = docID;
      // nocommit -- need not store in doc?  only used for alloc & assert
      currentDoc.termDocFreq = termDocFreq;
      if (termDocFreq > currentDoc.positions.length) {
        currentDoc.reallocPositions(termDocFreq);
      }
      currentDoc.numPositions = 0;
      if (omitTF) {
        return null;
      } else {
        return posWriter;
      }
    }
  }

  boolean pendingIsIndexTerm;

  int pulsedCount;
  int nonPulsedCount;

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(int docCount, boolean isIndexTerm) throws IOException {

    if (Codec.DEBUG)
      System.out.println("PW: finishTerm pendingDocCount=" + pendingDocCount);

    pendingIsIndexTerm |= isIndexTerm;

    if (pulsed) {
      wrappedDocsWriter.finishTerm(docCount, pendingIsIndexTerm);
      pendingIsIndexTerm = false;
      pulsedCount++;
    } else {
      nonPulsedCount++;
      // OK, there were few enough occurrences for this
      // term, so we fully inline our postings data into
      // terms dict:
      int lastDocID = 0;
      for(int i=0;i<pendingDocCount;i++) {
        final Document doc = pendingDocs[i];
        final int delta = doc.docID - lastDocID;
        lastDocID = doc.docID;
        if (omitTF) {
          termsOut.writeVInt(delta);
        } else {
          assert doc.numPositions == doc.termDocFreq;
          if (doc.numPositions == 1)
            termsOut.writeVInt((delta<<1)|1);
          else {
            termsOut.writeVInt(delta<<1);
            termsOut.writeVInt(doc.numPositions);
          }

          // TODO: we could do better in encoding
          // payloadLength, eg, if it's always the same
          // across all terms
          int lastPosition = 0;
          int lastPayloadLength = -1;

          for(int j=0;j<doc.numPositions;j++) {
            final Position pos = doc.positions[j];
            final int delta2 = pos.pos - lastPosition;
            lastPosition = pos.pos;
            if (storePayloads) {
              if (pos.payloadLength != lastPayloadLength) {
                termsOut.writeVInt((delta2 << 1)|1);
                termsOut.writeVInt(pos.payloadLength);
                lastPayloadLength = pos.payloadLength;
              } else
                termsOut.writeVInt(delta2 << 1);
              if (pos.payloadLength > 0)
                termsOut.writeBytes(pos.payload, 0, pos.payloadLength);
            } else
              termsOut.writeVInt(delta2);
          }
        }
      }
    }

    pendingDocCount = 0;
  }

  @Override
  public void close() throws IOException {
    wrappedDocsWriter.close();
  }
}
