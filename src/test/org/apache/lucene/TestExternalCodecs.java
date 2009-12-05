package org.apache.lucene;

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

import org.apache.lucene.util.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.codecs.*;
import org.apache.lucene.index.codecs.standard.*;
import org.apache.lucene.index.codecs.pulsing.*;
import org.apache.lucene.store.*;
import java.util.*;
import java.io.*;

/* Intentionally outside of oal.index to verify fully
   external codecs work fine */

public class TestExternalCodecs extends LuceneTestCase {

  // For fun, test that we can override how terms are
  // sorted, and basic things still work -- this comparator
  // sorts in reversed unicode code point order:
  private static final TermRef.Comparator reverseUnicodeComparator = new TermRef.Comparator() {
      @Override
      public int compare(TermRef t1, TermRef t2) {
        byte[] b1 = t1.bytes;
        byte[] b2 = t2.bytes;
        int b1Stop;
        int b1Upto = t1.offset;
        int b2Upto = t2.offset;
        if (t1.length < t2.length) {
          b1Stop = t1.offset + t1.length;
        } else {
          b1Stop = t1.offset + t2.length;
        }
        while(b1Upto < b1Stop) {
          final int bb1 = b1[b1Upto++] & 0xff;
          final int bb2 = b2[b2Upto++] & 0xff;
          if (bb1 != bb2) {
            //System.out.println("cmp 1=" + t1 + " 2=" + t2 + " return " + (bb2-bb1));
            return bb2 - bb1;
          }
        }

        // One is prefix of another, or they are equal
        return t2.length-t1.length;
      }
    };

  // TODO
  //   - good improvement would be to write through to disk,
  //     and then load into ram from disk
  public static class RAMOnlyCodec extends Codec {

    // Postings state:
    static class RAMPostings extends FieldsProducer {
      final Map<String,RAMField> fieldToTerms = new TreeMap<String,RAMField>();

      @Override
      public Terms terms(String field) {
        return fieldToTerms.get(field);
      }

      @Override
      public FieldsEnum iterator() {
        return new RAMFieldsEnum(this);
      }

      @Override
      public void close() {
      }

      @Override
      public void loadTermsIndex() {
      }
    } 

    static class RAMField extends Terms {
      final String field;
      final SortedMap<String,RAMTerm> termToDocs = new TreeMap<String,RAMTerm>();
      RAMField(String field) {
        this.field = field;
      }

      @Override
      public long getUniqueTermCount() {
        return termToDocs.size();
      }

      @Override
      public TermsEnum iterator() {
        return new RAMTermsEnum(RAMOnlyCodec.RAMField.this);
      }

      @Override
      public TermRef.Comparator getTermComparator() {
        return reverseUnicodeComparator;
      }
    }

    static class RAMTerm {
      final String term;
      final List<RAMDoc> docs = new ArrayList<RAMDoc>();
      public RAMTerm(String term) {
        this.term = term;
      }
    }

    static class RAMDoc {
      final int docID;
      final int[] positions;
      public RAMDoc(int docID, int freq) {
        this.docID = docID;
        positions = new int[freq];
      }
    }

    // Classes for writing to the postings state
    private static class RAMFieldsConsumer extends FieldsConsumer {

      private final RAMPostings postings;
      private final RAMTermsConsumer termsConsumer = new RAMTermsConsumer();

      public RAMFieldsConsumer(RAMPostings postings) {
        this.postings = postings;
      }

      @Override
      public TermsConsumer addField(FieldInfo field) {
        RAMField ramField = new RAMField(field.name);
        postings.fieldToTerms.put(field.name, ramField);
        termsConsumer.reset(ramField);
        return termsConsumer;
      }

      @Override
      public void close() {
        // TODO: finalize stuff
      }
    }

    private static class RAMTermsConsumer extends TermsConsumer {
      private RAMField field;
      private final RAMDocsConsumer docsConsumer = new RAMDocsConsumer();
      RAMTerm current;
      
      void reset(RAMField field) {
        this.field = field;
      }
      
      @Override
      public DocsConsumer startTerm(TermRef text) {
        final String term = text.toString();
        current = new RAMTerm(term);
        docsConsumer.reset(current);
        return docsConsumer;
      }

      
      @Override
      public TermRef.Comparator getTermComparator() {
        return TermRef.getUTF8SortedAsUTF16Comparator();
      }

      @Override
      public void finishTerm(TermRef text, int numDocs) {
        // nocommit -- are we even called when numDocs == 0?
        if (numDocs > 0) {
          assert numDocs == current.docs.size();
          field.termToDocs.put(current.term, current);
        }
      }

      @Override
      public void finish() {
      }
    }

    public static class RAMDocsConsumer extends DocsConsumer {
      private RAMTerm term;
      private RAMDoc current;
      private final RAMPositionsConsumer positions = new RAMPositionsConsumer();

      public void reset(RAMTerm term) {
        this.term = term;
      }
      @Override
      public PositionsConsumer addDoc(int docID, int freq) {
        current = new RAMDoc(docID, freq);
        term.docs.add(current);
        positions.reset(current);
        return positions;
      }
    }

    public static class RAMPositionsConsumer extends PositionsConsumer {
      private RAMDoc current;
      int upto = 0;
      public void reset(RAMDoc doc) {
        current = doc;
        upto = 0;
      }

      @Override
      public void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) {
        if (payload != null) {
          throw new UnsupportedOperationException("can't handle payloads");
        }
        current.positions[upto++] = position;
      }

      @Override
      public void finishDoc() {
        assert upto == current.positions.length;
      }
    }


    // Classes for reading from the postings state
    static class RAMFieldsEnum extends FieldsEnum {
      private final RAMPostings postings;
      private final Iterator<String> it;
      private String current;

      public RAMFieldsEnum(RAMPostings postings) {
        this.postings = postings;
        this.it = postings.fieldToTerms.keySet().iterator();
      }

      @Override
      public String next() {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }
        return current;
      }

      @Override
      public TermsEnum terms() {
        return new RAMTermsEnum(postings.fieldToTerms.get(current));
      }
    }

    static class RAMTermsEnum extends TermsEnum {
      Iterator<String> it;
      String current;
      private final RAMField ramField;

      public RAMTermsEnum(RAMField field) {
        this.ramField = field;
      }
      
      @Override
      public TermRef.Comparator getTermComparator() {
        return TermRef.getUTF8SortedAsUTF16Comparator();
      }

      @Override
      public TermRef next() {
        if (it == null) {
          if (current == null) {
            it = ramField.termToDocs.keySet().iterator();
          } else {
            it = ramField.termToDocs.tailMap(current).keySet().iterator();
          }
        }
        if (it.hasNext()) {
          current = it.next();
          return new TermRef(current);
        } else {
          return null;
        }
      }

      @Override
      public SeekStatus seek(TermRef term) {
        current = term.toString();
        if (ramField.termToDocs.containsKey(current)) {
          return SeekStatus.FOUND;
        } else {
          // nocommit -- right?
          if (current.compareTo(ramField.termToDocs.lastKey()) > 0) {
            return SeekStatus.END;
          } else {
            return SeekStatus.NOT_FOUND;
          }
        }
      }

      @Override
      public SeekStatus seek(long ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }

      @Override
      public TermRef term() {
        // TODO: reuse TermRef
        return new TermRef(current);
      }

      @Override
      public int docFreq() {
        return ramField.termToDocs.get(current).docs.size();
      }

      @Override
      public DocsEnum docs(Bits skipDocs) {
        return new RAMDocsEnum(ramField.termToDocs.get(current), skipDocs);
      }
    }

    private static class RAMDocsEnum extends DocsEnum {
      private final RAMTerm ramTerm;
      private final Bits skipDocs;
      private final RAMPositionsEnum positions = new RAMPositionsEnum();
      private RAMDoc current;
      int upto = -1;

      public RAMDocsEnum(RAMTerm ramTerm, Bits skipDocs) {
        this.ramTerm = ramTerm;
        this.skipDocs = skipDocs;
      }

      @Override
      // nocommit: Is this ok? it always return NO_MORE_DOCS
      public int advance(int targetDocID) {
        do {
          nextDoc();
        } while (upto < ramTerm.docs.size() && current.docID < targetDocID);
        return NO_MORE_DOCS;
      }

      // TODO: override bulk read, for better perf

      @Override
      public int nextDoc() {
        while(true) {
          upto++;
          if (upto < ramTerm.docs.size()) {
            current = ramTerm.docs.get(upto);
            if (skipDocs == null || !skipDocs.get(current.docID)) {
              return current.docID;
            }
          } else {
            return NO_MORE_DOCS;
          }
        }
      }

      @Override
      public int freq() {
        return current.positions.length;
      }

      @Override
      public int docID() {
        return current.docID;
      }

      @Override
      public PositionsEnum positions() {
        positions.reset(current);
        return positions;
      }
    }

    private static final class RAMPositionsEnum extends PositionsEnum {
      private RAMDoc ramDoc;
      int upto;

      public void reset(RAMDoc ramDoc) {
        this.ramDoc = ramDoc;
        upto = 0;
      }

      @Override
      public int next() {
        return ramDoc.positions[upto++];
      }

      @Override
      public boolean hasPayload() {
        return false;
      }

      @Override
      public int getPayloadLength() {
        return 0;
      }

      @Override
      public byte[] getPayload(byte[] data, int offset) {
        return null;
      }
    }

    // Holds all indexes created
    private final Map<String,RAMPostings> state = new HashMap<String,RAMPostings>();

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState writeState) {
      RAMPostings postings = new RAMPostings();
      RAMFieldsConsumer consumer = new RAMFieldsConsumer(postings);
      synchronized(state) {
        state.put(writeState.segmentName, postings);
      }
      return consumer;
    }

    @Override
    public FieldsProducer fieldsProducer(Directory dir, FieldInfos fieldInfos, SegmentInfo si, int readBufferSize, int indexDivisor)
      throws IOException {
      return state.get(si.name);
    }

    @Override
    public void getExtensions(Collection extensions) {
    }

    @Override
    public void files(Directory dir, SegmentInfo segmentInfo, Collection files) {
    }
  }

  /** Simple Codec that dispatches field-specific codecs.
   *  You must ensure every field you index has a Codec, or
   *  the defaultCodec is non null.  Also, the separate
   *  codecs cannot conflict on file names.*/
  public static class PerFieldCodecWrapper extends Codec {
    private final Map<String,Codec> fields = new HashMap<String,Codec>();
    private final Codec defaultCodec;

    public PerFieldCodecWrapper(Codec defaultCodec) {
      name = "PerField";
      this.defaultCodec = defaultCodec;
    }

    public void add(String field, Codec codec) {
      fields.put(field, codec);
    }

    Codec getCodec(String field) {
      Codec codec = fields.get(field);
      if (codec != null) {
        return codec;
      } else {
        return defaultCodec;
      }
    }
      
    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      return new FieldsWriter(state);
    }

    private class FieldsWriter extends FieldsConsumer {
      private final SegmentWriteState state;
      private final Map<Codec,FieldsConsumer> codecs = new HashMap<Codec,FieldsConsumer>();
      private final Set<String> fieldsSeen = new TreeSet<String>();

      public FieldsWriter(SegmentWriteState state) {
        this.state = state;
      }

      @Override
      public TermsConsumer addField(FieldInfo field) throws IOException {
        fieldsSeen.add(field.name);
        Codec codec = getCodec(field.name);

        FieldsConsumer fields = codecs.get(codec);
        if (fields == null) {
          fields = codec.fieldsConsumer(state);
          codecs.put(codec, fields);
        }
        //System.out.println("field " + field.name + " -> codec " + codec);
        return fields.addField(field);
      }

      @Override
      public void close() throws IOException {
        Iterator<FieldsConsumer> it = codecs.values().iterator();
        while(it.hasNext()) {
          // nocommit -- catch exc and keep closing the rest?
          it.next().close();
        }
      }
    }

    private class FieldsReader extends FieldsProducer {

      private final Set<String> fields = new TreeSet();
      private final Map<Codec,FieldsProducer> codecs = new HashMap<Codec,FieldsProducer>();

      public FieldsReader(Directory dir, FieldInfos fieldInfos,
                          SegmentInfo si, int readBufferSize,
                          int indexDivisor) throws IOException {

        final int fieldCount = fieldInfos.size();
        for(int i=0;i<fieldCount;i++) {
          FieldInfo fi = fieldInfos.fieldInfo(i);
          if (fi.isIndexed) {
            fields.add(fi.name);
            Codec codec = getCodec(fi.name);
            if (!codecs.containsKey(codec)) {
              codecs.put(codec, codec.fieldsProducer(dir, fieldInfos, si, readBufferSize, indexDivisor));
            }
          }
        }
      }

      private final class FieldsIterator extends FieldsEnum {
        private final Iterator<String> it;
        private String current;

        public FieldsIterator() {
          it = fields.iterator();
        }

        @Override
        public String next() {
          if (it.hasNext()) {
            current = it.next();
          } else {
            current = null;
          }

          return current;
        }

        @Override
        public TermsEnum terms() throws IOException {
          Terms terms = codecs.get(getCodec(current)).terms(current);
          if (terms != null) {
            return terms.iterator();
          } else {
            return null;
          }
        }
      }
      
      @Override
      public FieldsEnum iterator() throws IOException {
        return new FieldsIterator();
      }

      @Override
      public Terms terms(String field) throws IOException {
        Codec codec = getCodec(field);

        FieldsProducer fields = codecs.get(codec);
        assert fields != null;
        return fields.terms(field);
      }

      @Override
      public void close() throws IOException {
        Iterator<FieldsProducer> it = codecs.values().iterator();
        while(it.hasNext()) {
          // nocommit -- catch exc and keep closing the rest?
          it.next().close();
        }
      }

      @Override
      public void loadTermsIndex() throws IOException {
        Iterator<FieldsProducer> it = codecs.values().iterator();
        while(it.hasNext()) {
          // nocommit -- catch exc and keep closing the rest?
          it.next().loadTermsIndex();
        }
      }
    }

    public FieldsProducer fieldsProducer(Directory dir, FieldInfos fieldInfos,
                                         SegmentInfo si, int readBufferSize,
                                         int indexDivisor)
      throws IOException {
      return new FieldsReader(dir, fieldInfos, si, readBufferSize, indexDivisor);
    }

    @Override
    public void files(Directory dir, SegmentInfo info, Collection files) throws IOException {
      Iterator<Codec> it = fields.values().iterator();
      Set<Codec> seen = new HashSet<Codec>();
      while(it.hasNext()) {
        final Codec codec = it.next();
        if (!seen.contains(codec)) {
          seen.add(codec);
          codec.files(dir, info, files);
        }
      }
    }

    @Override
    public void getExtensions(Collection extensions) {
      Iterator<Codec> it = fields.values().iterator();
      while(it.hasNext()) {
        final Codec codec = it.next();
        codec.getExtensions(extensions);
      }
    }
  }

  public static class MyCodecs extends Codecs {
    PerFieldCodecWrapper perField;

    MyCodecs() {
      Codec ram = new RAMOnlyCodec();
      Codec pulsing = new PulsingReverseTermsCodec();
      perField = new PerFieldCodecWrapper(ram);
      perField.add("field2", pulsing);
      perField.add("id", pulsing);
      register(perField);
    }
    
    @Override
    public Codec getWriter(SegmentWriteState state) {
      return perField;
    }
  }

  // copied from PulsingCodec, just changing the terms
  // comparator
  private static class PulsingReverseTermsCodec extends Codec {

    public PulsingReverseTermsCodec() {
      name = "PulsingReverseTerms";
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      // We wrap StandardDocsWriter, but any DocsConsumer
      // will work:
      StandardDocsConsumer docsWriter = new StandardDocsWriter(state);

      // Terms that have <= freqCutoff number of docs are
      // "pulsed" (inlined):
      final int freqCutoff = 1;
      StandardDocsConsumer pulsingWriter = new PulsingDocsWriter(state, freqCutoff, docsWriter);

      // Terms dict index
      StandardTermsIndexWriter indexWriter;
      boolean success = false;
      try {
        indexWriter = new SimpleStandardTermsIndexWriter(state);
        success = true;
      } finally {
        if (!success) {
          pulsingWriter.close();
        }
      }

      // Terms dict
      success = false;
      try {
        FieldsConsumer ret = new StandardTermsDictWriter(indexWriter, state, pulsingWriter, reverseUnicodeComparator);
        success = true;
        return ret;
      } finally {
        if (!success) {
          try {
            pulsingWriter.close();
          } finally {
            indexWriter.close();
          }
        }
      }
    }

    @Override
    public FieldsProducer fieldsProducer(Directory dir, FieldInfos fieldInfos, SegmentInfo si, int readBufferSize, int indexDivisor) throws IOException {

      // We wrap StandardDocsReader, but any DocsProducer
      // will work:
      StandardDocsProducer docs = new StandardDocsReader(dir, si, readBufferSize);
      StandardDocsProducer docsReader = new PulsingDocsReader(dir, si, readBufferSize, docs);

      // Terms dict index reader
      StandardTermsIndexReader indexReader;

      boolean success = false;
      try {
        indexReader = new SimpleStandardTermsIndexReader(dir,
                                                         fieldInfos,
                                                         si.name,
                                                         indexDivisor,
                                                         reverseUnicodeComparator);
        success = true;
      } finally {
        if (!success) {
          docs.close();
        }
      }

      // Terms dict reader
      success = false;
      try {
        FieldsProducer ret = new StandardTermsDictReader(indexReader,
                                                         dir, fieldInfos, si.name,
                                                         docsReader,
                                                         readBufferSize,
                                                         reverseUnicodeComparator);
        success = true;
        return ret;
      } finally {
        if (!success) {
          try {
            docs.close();
          } finally {
            indexReader.close();
          }
        }
      }
    }

    @Override
    public void files(Directory dir, SegmentInfo segmentInfo, Collection<String> files) throws IOException {
      StandardDocsReader.files(dir, segmentInfo, files);
      StandardTermsDictReader.files(dir, segmentInfo, files);
      SimpleStandardTermsIndexReader.files(dir, segmentInfo, files);
    }

    @Override
    public void getExtensions(Collection<String> extensions) {
      StandardCodec.getStandardExtensions(extensions);
    }
  }


  /*
    tests storing "id" and "field2" fields as pulsing codec,
    whose term sort is backwards unicode code point, and
    storing "field1" as a custom entirely-in-RAM codec
   */
  public void testPerFieldCodec() throws Exception {
    
    final int NUM_DOCS = 173;

    Directory dir = new MockRAMDirectory();
    IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, null, IndexWriter.MaxFieldLength.UNLIMITED,
                                    null, null, new MyCodecs());

    w.setMergeFactor(3);
    Document doc = new Document();
    // uses default codec:
    doc.add(new Field("field1", "this field uses the standard codec as the test", Field.Store.NO, Field.Index.ANALYZED));
    // uses pulsing codec:
    doc.add(new Field("field2", "this field uses the pulsing codec as the test", Field.Store.NO, Field.Index.ANALYZED));
    
    Field idField = new Field("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    for(int i=0;i<NUM_DOCS;i++) {
      idField.setValue(""+i);
      w.addDocument(doc);
      if ((i+1)%10 == 0) {
        w.commit();
      }
    }
    w.deleteDocuments(new Term("id", "77"));

    IndexReader r = w.getReader();
    IndexReader[] subs = r.getSequentialSubReaders();
    assertTrue(subs.length > 1);
    // test each segment
    for(int i=0;i<subs.length;i++) {
      //System.out.println("test i=" + i);
      testTermsOrder(subs[i]);
    }
    // test each multi-reader
    testTermsOrder(r);
    
    assertEquals(NUM_DOCS-1, r.numDocs());
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(NUM_DOCS-1, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(NUM_DOCS-1, s.search(new TermQuery(new Term("field2", "pulsing")), 1).totalHits);
    r.close();
    s.close();

    w.deleteDocuments(new Term("id", "44"));
    w.optimize();
    r = w.getReader();
    assertEquals(NUM_DOCS-2, r.maxDoc());
    assertEquals(NUM_DOCS-2, r.numDocs());
    s = new IndexSearcher(r);
    assertEquals(NUM_DOCS-2, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(NUM_DOCS-2, s.search(new TermQuery(new Term("field2", "pulsing")), 1).totalHits);
    assertEquals(1, s.search(new TermQuery(new Term("id", "76")), 1).totalHits);
    assertEquals(0, s.search(new TermQuery(new Term("id", "77")), 1).totalHits);
    assertEquals(0, s.search(new TermQuery(new Term("id", "44")), 1).totalHits);

    testTermsOrder(r);
    r.close();
    s.close();

    w.close();

    dir.close();
  }

  private void testTermsOrder(IndexReader r) throws Exception {

    // Verify sort order matches what my comparator said:
    TermRef lastTermRef = null;
    TermsEnum terms = r.fields().terms("id").iterator();
    //System.out.println("id terms:");
    while(true) {
      TermRef t = terms.next();
      if (t == null) {
        break;
      }
      //System.out.println("  " + t);
      if (lastTermRef == null) {
        lastTermRef = new TermRef(t);
      } else {
        assertTrue("terms in wrong order last=" + lastTermRef + " current=" + t, reverseUnicodeComparator.compare(lastTermRef, t) < 0);
        lastTermRef.copy(t);
      }
    }
  }
}
